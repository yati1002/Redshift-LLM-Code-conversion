DROP TABLE IF EXISTS acp_edw.edw.referral;

CREATE TABLE acp_edw.edw.referral
(
  edw_referral_key        BIGINT IDENTITY(1,1) NOT NULL,
  edw_enctr_key           BIGINT,
  enctr_id                varchar(50),
  referral_id             VARCHAR(50)  NOT NULL,
  bundle_id               VARCHAR(50),
  referral_type           VARCHAR(100),
  authoredon_dtm          TIMESTAMP,
  reqstr_type             VARCHAR(100),
  referral_sts            VARCHAR(100),
  last_upd_dtm            TIMESTAMP   NOT NULL,
  reqstr_id               VARCHAR(50),
  reqst_method            VARCHAR(100),
  subject_type            VARCHAR(100),
  subject_id              VARCHAR(50) distkey,
  intent_type             VARCHAR(100),
  referred_to_type        VARCHAR(100),
  referred_to_id          VARCHAR(50),
  delete_flg              boolean,
  workflow_run_id         VARCHAR(50) not null,
  file_nm                 VARCHAR(1000),
  created_by              VARCHAR(100) DEFAULT 'acp_system' NOT NULL,
  created_dt              TIMESTAMP DEFAULT getdate()   NOT NULL,
  updated_by              VARCHAR(100),
  updated_dt              TIMESTAMP,
  primary key(edw_referral_key)
)
  SORTKEY
    (
    enctr_id,
    referral_id
    );
