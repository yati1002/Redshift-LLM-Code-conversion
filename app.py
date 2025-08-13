import streamlit as st
import os
import tempfile
import io
from pathlib import Path
import traceback
import sys
import pandas as pd
import requests
import json
import zipfile

# Debug information for SDK availability
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config
    DATABRICKS_SDK_AVAILABLE = True
    SDK_VERSION = None
    try:
        import databricks.sdk
        SDK_VERSION = getattr(databricks.sdk, '__version__', 'Unknown')
    except:
        pass
except ImportError as e:
    DATABRICKS_SDK_AVAILABLE = False
    SDK_IMPORT_ERROR = str(e)

# UC Volume path
UC_VOLUME_PATH = "/Volumes/powerbisamples/accolade_ddl/rs_ddl"

# AI endpoint configuration
AI_ENDPOINT_URL = "https://adb-984752964297111.11.azuredatabricks.net/serving-endpoints/databricks-gpt-oss-120b/invocations"

def get_databricks_client():
    """Get Databricks workspace client"""
    try:
        if not DATABRICKS_SDK_AVAILABLE:
            st.error("Databricks SDK not available. Please check requirements.txt")
            return None
        
        # Initialize Databricks client with default authentication
        client = WorkspaceClient()
        return client
    except Exception as e:
        st.error(f"Failed to initialize Databricks client: {str(e)}")
        return None

def ensure_uc_volume_exists():
    """Ensure the UC volume directory exists and is accessible"""
    try:
        client = get_databricks_client()
        if not client:
            return False
        
        # Try to list the volume directory to test access
        try:
            files = client.files.list_directory_contents(UC_VOLUME_PATH)
            return True
        except Exception as e:
            if "does not exist" in str(e).lower():
                st.error(f"UC Volume path does not exist: {UC_VOLUME_PATH}")
                st.error("Please verify the volume path and ensure it's properly mounted.")
            else:
                st.error(f"Cannot access UC volume: {str(e)}")
                st.error("Please ensure you have proper permissions to access the Unity Catalog volume.")
            return False
            
    except Exception as e:
        st.error(f"Failed to check UC volume: {str(e)}")
        return False

def ai_query(prompt, max_tokens=4000):
    """Query the Databricks AI endpoint"""
    try:
        # Get access token from session state
        access_token = st.session_state.get('databricks_token')
        
        if not access_token:
            st.error("❌ No access token provided")
            st.info("💡 Please enter your Databricks access token in the sidebar")
            return None
            
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "messages": [
                {
                    "role": "user", 
                    "content": prompt
                }
            ],
            "max_tokens": max_tokens,
            "temperature": 0.1
        }
        
        response = requests.post(AI_ENDPOINT_URL, headers=headers, json=payload, timeout=60)
        
        if response.status_code == 200:
            result = response.json()

            # Normalize content from multiple possible response shapes
            content = ""
            try:
                if isinstance(result, dict) and "choices" in result and result["choices"]:
                    first_choice = result["choices"][0]
                    if isinstance(first_choice, dict):
                        if "message" in first_choice and isinstance(first_choice["message"], dict):
                            raw = first_choice["message"].get("content", "")
                            if isinstance(raw, list):
                                # Join list parts (e.g., [{'type':'text','text':'...'}])
                                parts = []
                                for seg in raw:
                                    if isinstance(seg, str):
                                        parts.append(seg)
                                    elif isinstance(seg, dict):
                                        parts.append(seg.get("text") or seg.get("content") or "")
                                    else:
                                        parts.append(str(seg))
                                content = "".join(parts)
                            else:
                                content = raw or ""
                        elif "text" in first_choice:
                            content = first_choice.get("text", "")
                        else:
                            content = str(first_choice)
                elif isinstance(result, dict) and "text" in result:
                    content = result.get("text", "")
                elif isinstance(result, dict) and "response" in result:
                    content = result.get("response", "")
                elif isinstance(result, dict) and "predictions" in result and result["predictions"]:
                    pred = result["predictions"][0]
                    if isinstance(pred, dict):
                        content = pred.get("text") or pred.get("output") or str(pred)
                    else:
                        content = str(pred)
                else:
                    content = str(result)
            except Exception:
                # Fallback to stringified result if normalization fails
                content = str(result)
            
            # Ensure string
            if not isinstance(content, str):
                content = str(content)
            
            # Clean up the response - remove ```sql and ``` markers
            content = content.strip()
            if content.startswith('```sql'):
                content = content[6:]
            if content.startswith('```'):
                content = content[3:]
            if content.endswith('```'):
                content = content[:-3]
            content = content.strip()
            
            return content
        else:
            st.error(f"AI API Error: {response.status_code}")
            st.error(f"Response text: {response.text}")
            return None
            
    except Exception as e:
        st.error(f"Error calling AI endpoint: {str(e)}")
        return None

def create_conversion_prompt(sql_content):
    """Create an intelligent prompt for converting DDL to Databricks SQL"""
    prompt = f"""
You are an expert SQL developer specializing in converting SQL DDL statements to Databricks SQL format.

Please convert the following SQL DDL code to be fully compatible with Databricks SQL syntax and best practices based on the official Databricks documentation:

Original SQL:
{sql_content}

IMPORTANT CONVERSION RULES:

1. DATA TYPES:
   - Keep VARCHAR(n) as VARCHAR(n) (Databricks supports VARCHAR)
   - Keep CHAR(n) as CHAR(n) if needed, or VARCHAR(n)
   - Keep TEXT as TEXT, or convert to VARCHAR if appropriate
   - Keep BIGINT, INT, SMALLINT, TINYINT as-is
   - Keep BOOLEAN, TIMESTAMP, DATE as-is
   - Keep DECIMAL(p,s) as DECIMAL(p,s)
   - FLOAT/REAL → FLOAT
   - DOUBLE PRECISION → DOUBLE
   - STRING is also valid if used

2. IDENTITY COLUMNS:
   - BIGINT IDENTITY(1,1) → BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)
   - INT IDENTITY(1,1) → INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)
   - Custom start/increment: IDENTITY(start,increment) → GENERATED ALWAYS AS IDENTITY (START WITH start INCREMENT BY increment)

3. CONSTRAINTS:
   - PRIMARY KEY (column) → PRIMARY KEY (column) 
   - FOREIGN KEY → Remove (not supported in Delta tables)
   - UNIQUE → Remove (not supported in Delta tables)
   - CHECK constraints → Remove (not supported in Delta tables)

4. DEFAULT VALUES:
   - Keep DEFAULT values in CREATE TABLE but move to correct position: column_name TYPE NOT NULL DEFAULT value
   - getdate() → CURRENT_TIMESTAMP()
   - getutcdate() → CURRENT_TIMESTAMP()
   - SYSDATE → CURRENT_TIMESTAMP()

5. DISTRIBUTION/SORTING (Remove these - they are platform-specific):
   - Remove DISTKEY, SORTKEY, DISTSTYLE (Redshift-specific)
   - Remove CLUSTERED/NONCLUSTERED (SQL Server-specific)
   - Remove PARTITION BY, ORDER BY in table definition (unless it's proper partitioning)

6. TABLE PROPERTIES:
   - Add USING DELTA
   - Add TBLPROPERTIES:
     * If IDENTITY columns exist: 'delta.feature.identityColumns' = 'supported'
     * If DEFAULT values exist: 'delta.feature.allowColumnDefaults' = 'supported'

7. SCHEMA NAMES:
   - Keep three-part names (catalog.schema.table) as-is
   - Convert two-part names appropriately

REQUIRED OUTPUT FORMAT:
1. Single CREATE TABLE statement with USING DELTA
2. Include TBLPROPERTIES section if IDENTITY or DEFAULT values are present
3. Do NOT create separate ALTER TABLE statements for DEFAULT values - keep them in CREATE TABLE
4. Do NOT include ```sql or ``` in your response
5. Provide clean SQL code only

EXAMPLE CONVERSION:
Input: 
CREATE TABLE test (id BIGINT IDENTITY(1,1), name VARCHAR(50) DEFAULT 'unknown', created_dt TIMESTAMP DEFAULT getdate())

Output:
CREATE TABLE test (
  id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  name VARCHAR(50) NOT NULL DEFAULT 'unknown',
  created_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
TBLPROPERTIES (
  'delta.feature.identityColumns' = 'supported',
  'delta.feature.allowColumnDefaults' = 'supported'
);

Please convert the SQL and return ONLY the converted Databricks SQL code without any explanations.
"""
    return prompt

def upload_file_to_uc_volume(uploaded_file, client):
    """Upload file to UC volume using Databricks SDK"""
    try:
        destination_path = f"{UC_VOLUME_PATH}/{uploaded_file.name}"
        
        # Get file content as bytes
        file_content = uploaded_file.getvalue()
        
        # Upload file using Databricks SDK - correct method
        client.files.upload(
            file_path=destination_path,
            contents=io.BytesIO(file_content),
            overwrite=True
        )
        
        return True
    except Exception as e:
        st.error(f"Error uploading file {uploaded_file.name}: {str(e)}")
        # Show more detailed error for debugging
        st.error(f"Detailed error: {traceback.format_exc()}")
        return False

def read_file_content(file_path, client):
    """Read file content from UC volume"""
    try:
        # Download file content
        file_response = client.files.download(file_path)
        
        # Read the content as text
        content = file_response.contents.read().decode('utf-8')
        return content
    except Exception as e:
        st.error(f"Error reading file {file_path}: {str(e)}")
        return None

def get_sql_files():
    """Get list of .sql files from UC volume"""
    try:
        client = get_databricks_client()
        if not client:
            return []
        
        files = client.files.list_directory_contents(UC_VOLUME_PATH)
        file_list = list(files)
        
        sql_files = []
        for file_info in file_list:
            # Check if it's a file (not a directory) and has .sql extension
            if hasattr(file_info, 'is_directory') and not file_info.is_directory:
                file_name = file_info.path.split('/')[-1]
                if file_name.lower().endswith('.sql'):
                    sql_files.append(file_info)
            elif not hasattr(file_info, 'is_directory'):
                # Fallback: check for .sql extension
                file_name = file_info.path.split('/')[-1]
                if file_name.lower().endswith('.sql'):
                    sql_files.append(file_info)
        
        return sql_files
    except Exception as e:
        st.error(f"Error listing SQL files: {str(e)}")
        return []

def show_current_files():
    """Display current files in the UC volume"""
    try:
        client = get_databricks_client()
        if not client:
            return
        
        files = client.files.list_directory_contents(UC_VOLUME_PATH)
        file_list = list(files)
        
        if file_list:
            st.subheader("Files in UC Volume:")
            for file_info in sorted(file_list, key=lambda x: x.path):
                # Check if it's a file (not a directory) using the correct attribute
                if hasattr(file_info, 'is_directory') and not file_info.is_directory:
                    file_size = getattr(file_info, 'file_size', 0) or 0
                    file_name = file_info.path.split('/')[-1]
                    st.write(f"📄 {file_name} ({file_size / 1024:.2f} KB)")
                elif not hasattr(file_info, 'is_directory'):
                    # Fallback: if no is_directory attribute, assume it's a file if it has a file extension
                    file_name = file_info.path.split('/')[-1]
                    if '.' in file_name:  # Simple check for file extension
                        file_size = getattr(file_info, 'file_size', 0) or 0
                        st.write(f"📄 {file_name} ({file_size / 1024:.2f} KB)")
                    else:
                        st.write(f"📁 {file_name} (directory)")
        else:
            st.info("No files found in UC volume")
            
    except Exception as e:
        st.error(f"Error reading UC volume: {str(e)}")
        # Show more detailed error for debugging
        st.error(f"Detailed error: {traceback.format_exc()}")
        
        # Add debug info about the DirectoryEntry object structure
        try:
            client = get_databricks_client()
            if client:
                files = client.files.list_directory_contents(UC_VOLUME_PATH)
                file_list = list(files)
                if file_list:
                    sample_entry = file_list[0]
                    st.write("**Debug - DirectoryEntry attributes:**")
                    st.code(f"Available attributes: {[attr for attr in dir(sample_entry) if not attr.startswith('_')]}")
                    st.code(f"Sample entry: {sample_entry}")
        except:
            pass

def upload_files(uploaded_files):
    """Handle the upload process for multiple files"""
    if not uploaded_files:
        return
    
    client = get_databricks_client()
    if not client:
        return
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    total_files = len(uploaded_files)
    successful_uploads = 0
    failed_uploads = []
    
    for i, uploaded_file in enumerate(uploaded_files):
        # Update progress
        progress = (i + 1) / total_files
        progress_bar.progress(progress)
        status_text.text(f"Uploading {uploaded_file.name}...")
        
        # Upload the file
        if upload_file_to_uc_volume(uploaded_file, client):
            successful_uploads += 1
            st.success(f"✅ Successfully uploaded: {uploaded_file.name}")
        else:
            failed_uploads.append(uploaded_file.name)
    
    # Final status
    status_text.text("Upload completed!")
    
    # Summary
    st.subheader("Upload Summary")
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric(
            label="Successful Uploads",
            value=successful_uploads,
            delta=f"{successful_uploads}/{total_files}"
        )
    
    with col2:
        st.metric(
            label="Failed Uploads",
            value=len(failed_uploads),
            delta=f"{len(failed_uploads)}/{total_files}" if failed_uploads else "0"
        )
    
    if failed_uploads:
        st.error("Failed to upload the following files:")
        for file_name in failed_uploads:
            st.write(f"❌ {file_name}")

def show_debug_info():
    """Show debug information in an expander"""
    with st.expander("🔧 Debug Information", expanded=False):
        st.write(f"**Python Version:** {sys.version}")
        st.write(f"**Databricks SDK Available:** {DATABRICKS_SDK_AVAILABLE}")
        
        if DATABRICKS_SDK_AVAILABLE:
            st.write(f"**SDK Version:** {SDK_VERSION}")
            
            # Test client initialization
            try:
                client = WorkspaceClient()
                st.success("✅ WorkspaceClient initialized successfully")
                
                # Test files service availability
                try:
                    if hasattr(client, 'files'):
                        st.success("✅ Files service available")
                        
                        # Test available methods
                        methods = [method for method in dir(client.files) if not method.startswith('_')]
                        st.write(f"**Available methods:** {', '.join(methods[:10])}...")
                    else:
                        st.error("❌ Files service not available")
                except Exception as e:
                    st.error(f"❌ Error testing files service: {e}")
                    
            except Exception as e:
                st.error(f"❌ Error initializing WorkspaceClient: {e}")
        else:
            st.write(f"**Import Error:** {globals().get('SDK_IMPORT_ERROR', 'Unknown')}")
        
        st.write("**Installed Packages:**")
        try:
            import subprocess
            result = subprocess.run([sys.executable, "-m", "pip", "list"], 
                                  capture_output=True, text=True, timeout=10)
            if "databricks-sdk" in result.stdout:
                for line in result.stdout.split('\n'):
                    if 'databricks' in line.lower():
                        st.code(line)
            else:
                st.error("databricks-sdk not found in installed packages")
        except Exception as e:
            st.write(f"Could not check installed packages: {e}")

def analyze_conversion_changes(original_sql, converted_sql):
    """Analyze the changes made during SQL conversion and return a summary"""
    changes = []
    
    original_upper = original_sql.upper()
    converted_upper = converted_sql.upper()
    
    # Data type conversions (corrected for Databricks)
    if 'VARCHAR' in original_upper and 'STRING' in converted_upper:
        changes.append("VARCHAR → STRING")
    elif 'VARCHAR' in original_upper and 'VARCHAR' in converted_upper:
        changes.append("Preserved VARCHAR types")
    
    if 'CHAR(' in original_upper and 'VARCHAR' in converted_upper:
        changes.append("CHAR → VARCHAR")
    
    if 'TEXT' in original_upper and ('VARCHAR' in converted_upper or 'STRING' in converted_upper):
        changes.append("TEXT → VARCHAR/STRING")
    
    # Identity columns (corrected conversion)
    if 'IDENTITY(' in original_upper and 'GENERATED ALWAYS AS IDENTITY' in converted_upper:
        changes.append("IDENTITY → GENERATED ALWAYS AS IDENTITY")
    elif 'IDENTITY(' in original_upper:
        changes.append("Converted IDENTITY columns")
    
    # Default values (now kept in CREATE TABLE)
    if 'DEFAULT' in original_upper and 'DEFAULT' in converted_upper:
        changes.append("Preserved DEFAULT values in CREATE TABLE")
    
    if 'GETDATE()' in original_upper and 'CURRENT_TIMESTAMP()' in converted_upper:
        changes.append("getdate() → CURRENT_TIMESTAMP()")
    
    if 'GETUTCDATE()' in original_upper and 'CURRENT_TIMESTAMP()' in converted_upper:
        changes.append("getutcdate() → CURRENT_TIMESTAMP()")
    
    if 'SYSDATE' in original_upper and 'CURRENT_TIMESTAMP()' in converted_upper:
        changes.append("SYSDATE → CURRENT_TIMESTAMP()")
    
    # Constraints
    if 'PRIMARY KEY' in original_upper and 'PRIMARY KEY' in converted_upper:
        changes.append("Preserved PRIMARY KEY")
    
    if 'FOREIGN KEY' in original_upper and 'FOREIGN KEY' not in converted_upper:
        changes.append("Removed FOREIGN KEY (not supported)")
    
    if 'UNIQUE' in original_upper and 'UNIQUE' not in converted_upper:
        changes.append("Removed UNIQUE constraints (not supported)")
    
    if 'CHECK(' in original_upper and 'CHECK(' not in converted_upper:
        changes.append("Removed CHECK constraints (not supported)")
    
    # Distribution keys (platform-specific removals)
    if 'DISTKEY' in original_upper:
        changes.append("Removed DISTKEY (Redshift-specific)")
    
    if 'SORTKEY' in original_upper:
        changes.append("Removed SORTKEY (Redshift-specific)")
    
    if 'DISTSTYLE' in original_upper:
        changes.append("Removed DISTSTYLE (Redshift-specific)")
    
    if 'CLUSTERED' in original_upper or 'NONCLUSTERED' in original_upper:
        changes.append("Removed CLUSTERED/NONCLUSTERED (SQL Server-specific)")
    
    # Table format and properties
    if 'USING DELTA' in converted_upper:
        changes.append("Added USING DELTA")
    
    if 'TBLPROPERTIES' in converted_upper:
        if 'IDENTITYCOLUMNS' in converted_upper.replace(' ', ''):
            changes.append("Added IDENTITY columns support")
        if 'ALLOWCOLUMNDEFAULTS' in converted_upper.replace(' ', ''):
            changes.append("Added column defaults support")
    
    # If no specific changes detected, provide general summary
    if not changes:
        changes.append("Converted to Databricks SQL format")
    
    return "; ".join(changes)

def file_upload_tab():
    """Content for the file upload tab"""
    st.header("📁 Upload Files")
    
    # Multiple file upload
    uploaded_files = st.file_uploader(
        "Choose files to upload",
        accept_multiple_files=True,
        help="Select one or more files to upload to the UC volume"
    )
    
    if uploaded_files:
        st.subheader("Selected Files:")
        
        # Display selected files
        for i, file in enumerate(uploaded_files):
            col1, col2, col3 = st.columns([3, 2, 1])
            with col1:
                st.write(f"**{file.name}**")
            with col2:
                st.write(f"Size: {file.size / 1024:.2f} KB")
            with col3:
                st.write(f"Type: {file.type or 'Unknown'}")
        
        # Upload button
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("🚀 Upload Files", type="primary", use_container_width=True):
                upload_files(uploaded_files)

def file_reader_tab():
    """Content for the file reader tab"""
    st.header("📖 Read SQL File Content")
    
    # Add access token input
    st.subheader("🔑 Databricks Access Token")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        token_input = st.text_input(
            "Enter your Databricks access token:",
            type="password",
            value=st.session_state.get('databricks_token', ''),
            help="This token will be used to call the AI endpoint for SQL conversion",
            placeholder="dapi-..."
        )
    with col2:
        if st.button("💾 Save Token"):
            if token_input.strip():
                st.session_state.databricks_token = token_input.strip()
                st.success("✅ Token saved!")
            else:
                st.error("❌ Please enter a valid token")
    
    # Show token status
    if st.session_state.get('databricks_token'):
        token_length = len(st.session_state.databricks_token)
        st.success(f"✅ Token is set (length: {token_length})")
    else:
        st.warning("⚠️ No access token set - AI conversion will not work")
    
    # Add AI connection test
    st.subheader("🤖 AI Connection Test")
    col_test1, col_test2 = st.columns(2)
    with col_test1:
        if st.button("🔧 Test AI Connection", help="Test if the AI endpoint is working"):
            test_ai_connection()
    with col_test2:
        st.info("💡 Test the AI connection before running conversions")
    
    st.divider()
    
    # Get SQL files
    sql_files = get_sql_files()
    
    if not sql_files:
        st.info("No .sql files found in the UC volume")
        if st.button("🔄 Refresh SQL Files"):
            st.rerun()
        return
    
    st.success(f"Found {len(sql_files)} SQL file(s)")
    
    # Buttons for actions
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Refresh SQL Files"):
            st.rerun()
    with col2:
        load_all = st.button("📖 Load All SQL Files", type="primary")
    
    if load_all:
        # Create progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Prepare data for table
        file_data = []
        client = get_databricks_client()
        
        if client:
            for i, file_info in enumerate(sql_files):
                # Update progress
                progress = (i + 1) / len(sql_files)
                progress_bar.progress(progress)
                
                file_name = file_info.path.split('/')[-1]
                status_text.text(f"Reading {file_name}...")
                
                # Read file content
                content = read_file_content(file_info.path, client)
                
                if content is not None:
                    file_data.append({
                        "Filename": file_name,
                        "Content": content,
                        "Converted DBSQL Code": "",  # Initially empty
                        "Comments": ""  # Initially empty
                    })
                else:
                    file_data.append({
                        "Filename": file_name,
                        "Content": "❌ Error reading file",
                        "Converted DBSQL Code": "",
                        "Comments": ""
                    })
            
            status_text.text("Loading complete!")
            
            # Display as DataFrame
            if file_data:
                st.subheader("SQL Files Content")
                
                # Store dataframe in session state for persistence
                if 'file_data_df' not in st.session_state:
                    st.session_state.file_data_df = pd.DataFrame(file_data)
                else:
                    # Update only if new data is loaded
                    st.session_state.file_data_df = pd.DataFrame(file_data)
                
                df = st.session_state.file_data_df
                
                # Display table with custom styling
                st.dataframe(
                    df,
                    use_container_width=True,
                    height=400,
                    column_config={
                        "Filename": st.column_config.TextColumn("Filename", width="small"),
                        "Content": st.column_config.TextColumn("Content", width="large"),
                        "Converted DBSQL Code": st.column_config.TextColumn("Converted DBSQL Code", width="large"),
                        "Comments": st.column_config.TextColumn("Comments", width="medium")
                    }
                )
                
                # Show current conversion status
                if 'Converted DBSQL Code' in df.columns:
                    converted_files = df[df['Converted DBSQL Code'].str.strip() != ""].shape[0]
                    total_files = len(df)
                    st.info(f"📊 Conversion Status: {converted_files}/{total_files} files converted")
                
                # Convert to DBSQL button
                col1, col2 = st.columns(2)
                
                with col1:
                    # Check if token is available before showing convert button
                    if not st.session_state.get('databricks_token'):
                        st.button("🤖 Convert to DBSQL", disabled=True, help="Please enter access token first", use_container_width=True)
                    else:
                        if st.button("🤖 Convert to DBSQL", type="primary", use_container_width=True, key="convert_button"):
                            st.info("🚀 Starting conversion process...")
                            
                            # Create progress bar for conversion
                            convert_progress = st.progress(0)
                            convert_status = st.empty()
                            
                            total_rows = len(df)
                            converted_count = 0
                            
                            # Create a copy of the dataframe to work with
                            updated_df = df.copy()
                            
                            for idx, row in df.iterrows():
                                if row['Content'] and row['Content'] != "❌ Error reading file":
                                    # Update progress
                                    progress = (idx + 1) / total_rows
                                    convert_progress.progress(progress)
                                    convert_status.text(f"Converting {row['Filename']} ({idx + 1}/{total_rows})...")
                                    
                                    # Create prompt and call AI
                                    prompt = create_conversion_prompt(row['Content'])
                                    
                                    # Call AI
                                    converted_code = ai_query(prompt)
                                    
                                    if converted_code and converted_code.strip():
                                        # Update the dataframe
                                        updated_df.loc[idx, 'Converted DBSQL Code'] = converted_code
                                        
                                        # Analyze changes and add comments
                                        comments = analyze_conversion_changes(row['Content'], converted_code)
                                        updated_df.loc[idx, 'Comments'] = comments
                                        
                                        converted_count += 1
                                        st.success(f"✅ Converted {row['Filename']}")
                                    else:
                                        updated_df.loc[idx, 'Converted DBSQL Code'] = "❌ Conversion failed"
                                        updated_df.loc[idx, 'Comments'] = "AI conversion failed"
                                        st.error(f"❌ Failed to convert {row['Filename']}")
                                else:
                                    convert_status.text(f"Skipping {row['Filename']} (no content)...")
                                    updated_df.loc[idx, 'Converted DBSQL Code'] = "❌ No content to convert"
                                    updated_df.loc[idx, 'Comments'] = "No content available"
                            
                            # Update session state with the converted data
                            st.session_state.file_data_df = updated_df
                            
                            convert_status.text("Conversion complete!")
                            st.success(f"✅ Conversion complete! Successfully converted {converted_count}/{total_rows} files.")
                            
                            # Force a rerun to show updated table
                            st.rerun()
                
                with col2:
                    # Check if any conversions exist
                    has_conversions = any(df['Converted DBSQL Code'].str.strip() != "")
                    
                    if has_conversions:
                        # Create ZIP file for download
                        zip_buffer = create_download_zip(df)
                        
                        st.download_button(
                            label="📥 Download Converted Code",
                            data=zip_buffer,
                            file_name="converted_dbsql_files.zip",
                            mime="application/zip",
                            use_container_width=True
                        )
                    else:
                        st.button("📥 Download Converted Code", disabled=True, help="Run conversion first", use_container_width=True)
    
    # Show existing session data if available
    elif 'file_data_df' in st.session_state and not st.session_state.file_data_df.empty:
        st.subheader("Previously Loaded SQL Files")
        df = st.session_state.file_data_df
        
        # Display table
        st.dataframe(
            df,
            use_container_width=True,
            height=400,
            column_config={
                "Filename": st.column_config.TextColumn("Filename", width="small"),
                "Content": st.column_config.TextColumn("Content", width="large"),
                "Converted DBSQL Code": st.column_config.TextColumn("Converted DBSQL Code", width="large"),
                "Comments": st.column_config.TextColumn("Comments", width="medium")
            }
        )
        
        # Show conversion status
        converted_files = df[df['Converted DBSQL Code'].str.strip() != ""].shape[0]
        total_files = len(df)
        st.info(f"📊 Conversion Status: {converted_files}/{total_files} files converted")
        
        # Convert to DBSQL button for existing data
        col1, col2 = st.columns(2)
        
        with col1:
            # Check if token is available before showing convert button
            if not st.session_state.get('databricks_token'):
                st.button("🤖 Convert to DBSQL", disabled=True, help="Please enter access token first", use_container_width=True)
            else:
                if st.button("🤖 Convert to DBSQL", type="primary", use_container_width=True, key="convert_existing_button"):
                    st.info("🚀 Starting conversion process...")
                    
                    # Create progress bar for conversion
                    convert_progress = st.progress(0)
                    convert_status = st.empty()
                    
                    total_rows = len(df)
                    converted_count = 0
                    
                    # Create a copy of the dataframe to work with
                    updated_df = df.copy()
                    
                    for idx, row in df.iterrows():
                        if row['Content'] and row['Content'] != "❌ Error reading file":
                            # Update progress
                            progress = (idx + 1) / total_rows
                            convert_progress.progress(progress)
                            convert_status.text(f"Converting {row['Filename']} ({idx + 1}/{total_rows})...")
                            
                            # Create prompt and call AI
                            prompt = create_conversion_prompt(row['Content'])
                            
                            # Call AI
                            converted_code = ai_query(prompt)
                            
                            if converted_code and converted_code.strip():
                                # Update the dataframe
                                updated_df.loc[idx, 'Converted DBSQL Code'] = converted_code
                                
                                # Analyze changes and add comments
                                comments = analyze_conversion_changes(row['Content'], converted_code)
                                updated_df.loc[idx, 'Comments'] = comments
                                
                                converted_count += 1
                                st.success(f"✅ Converted {row['Filename']}")
                            else:
                                updated_df.loc[idx, 'Converted DBSQL Code'] = "❌ Conversion failed"
                                updated_df.loc[idx, 'Comments'] = "AI conversion failed"
                                st.error(f"❌ Failed to convert {row['Filename']}")
                        else:
                            convert_status.text(f"Skipping {row['Filename']} (no content)...")
                            updated_df.loc[idx, 'Converted DBSQL Code'] = "❌ No content to convert"
                            updated_df.loc[idx, 'Comments'] = "No content available"
                    
                    # Update session state with the converted data
                    st.session_state.file_data_df = updated_df
                    
                    convert_status.text("Conversion complete!")
                    st.success(f"✅ Conversion complete! Successfully converted {converted_count}/{total_rows} files.")
                    
                    # Force a rerun to show updated table
                    st.rerun()
        
        with col2:
            # Check if any conversions exist
            has_conversions = any(df['Converted DBSQL Code'].str.strip() != "")
            
            if has_conversions:
                # Create ZIP file for download
                zip_buffer = create_download_zip(df)
                
                st.download_button(
                    label="📥 Download Converted Code",
                    data=zip_buffer,
                    file_name="converted_dbsql_files.zip",
                    mime="application/zip",
                    use_container_width=True
                )
            else:
                st.button("📥 Download Converted Code", disabled=True, help="Run conversion first", use_container_width=True)
    
    else:
        # Show list of available SQL files
        st.subheader("Available SQL Files:")
        for file_info in sql_files:
            file_name = file_info.path.split('/')[-1]
            file_size = getattr(file_info, 'file_size', 0) or 0
            
            # Create expandable section for each file
            with st.expander(f"📄 {file_name} ({file_size / 1024:.2f} KB)"):
                if st.button(f"Read {file_name}", key=f"read_{file_name}"):
                    client = get_databricks_client()
                    if client:
                        with st.spinner(f"Reading {file_name}..."):
                            content = read_file_content(file_info.path, client)
                            if content:
                                st.code(content, language="sql")
                            else:
                                st.error(f"Failed to read {file_name}")

def create_download_zip(df):
    """Create a ZIP file with individual SQL files for each converted code"""
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for index, row in df.iterrows():
            if pd.notna(row.get('Converted DBSQL Code')):
                # Create filename: original_name_converteddbsqlcode.sql
                original_filename = row['Filename']
                base_name = original_filename.rsplit('.', 1)[0]  # Remove extension
                new_filename = f"{base_name}_converteddbsqlcode.sql"
                
                # Add file to ZIP
                zip_file.writestr(new_filename, row['Converted DBSQL Code'])
    
    zip_buffer.seek(0)
    return zip_buffer

def test_ai_connection():
    """Test the AI endpoint connection"""
    try:
        # Check if token is provided
        if not st.session_state.get('databricks_token'):
            st.error("❌ Please enter your Databricks access token first")
            return False
            
        st.info("🔍 Testing AI connection...")
        
        # Test simple prompt
        test_prompt = "Convert this SQL to Databricks format: CREATE TABLE test (id INT, name VARCHAR(50));"
        
        with st.spinner("Calling AI endpoint..."):
            result = ai_query(test_prompt)
        
        if result and result.strip():
            st.success("✅ AI endpoint test successful!")
            return True
        else:
            st.error("❌ AI endpoint test failed")
            return False
            
    except Exception as e:
        st.error(f"❌ Test failed: {str(e)}")
        return False

def main():
    st.set_page_config(
        page_title="UC Volume File Manager",
        page_icon="📁",
        layout="wide"
    )
    
    st.title("🗂️ Unity Catalog Volume File Manager")
    st.markdown(f"**UC Volume Path:** `{UC_VOLUME_PATH}`")
    
    # Show debug information
    show_debug_info()
    
    # Check SDK availability
    if not DATABRICKS_SDK_AVAILABLE:
        st.error("❌ Databricks SDK not available")
        st.error("Please ensure the app has been redeployed with updated requirements.txt")
        st.info("💡 **Troubleshooting Steps:**")
        st.info("1. Redeploy the app to install new dependencies")
        st.info("2. Check that requirements.txt contains: databricks-sdk==0.28.0")
        st.info("3. Wait for the deployment to complete fully")
        st.stop()
    
    # Check if UC volume is accessible
    if not ensure_uc_volume_exists():
        st.stop()
    
    st.success("✅ UC Volume is accessible!")
    
    # Create tabs
    tab1, tab2 = st.tabs(["📁 File Upload to Unity Catalog Volume", "📖 Read Unity Catalog File Content"])
    
    with tab1:
        file_upload_tab()
    
    with tab2:
        file_reader_tab()
    
    # Sidebar with information
    with st.sidebar:
        st.header("ℹ️ Information")
        st.markdown(f"""
        **UC Volume Path:**
        ```
        {UC_VOLUME_PATH}
        ```
        
        **Features:**
        - Multiple file upload via Databricks SDK
        - Upload progress tracking
        - Read SQL file content
        - AI-powered DDL conversion to Databricks SQL
        - Display content in table format
        - Download converted code as individual files
        - Error handling
        """)
        
        if DATABRICKS_SDK_AVAILABLE and SDK_VERSION:
            st.success(f"✅ Databricks SDK v{SDK_VERSION}")
        
        # Token status
        st.header("🔑 Access Token Status")
        if st.session_state.get('databricks_token'):
            token_length = len(st.session_state.databricks_token)
            st.success(f"✅ Token is set (length: {token_length})")
            
            if st.button("🗑️ Clear Token"):
                del st.session_state.databricks_token
                st.rerun()
        else:
            st.warning("⚠️ No access token set")
            st.info("💡 Go to the 'Read Unity Catalog File Content' tab to set your token")
        
        st.header("📂 Current Files")
        if st.button("🔄 Refresh File List"):
            show_current_files()
        else:
            show_current_files()

if __name__ == "__main__":
    main()
