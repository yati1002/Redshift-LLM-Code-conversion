DROP TABLE IF EXISTS acp_edw.edw.referral_sts_hist;

CREATE TABLE acp_edw.edw.referral_sts_hist (
  edw_referral_sts_hist_key      BIGINT IDENTITY(1,1) NOT NULL,
  edw_referral_key               BIGINT,
  referral_id                    VARCHAR(50) NOT NULL DISTKEY,
  reqst_method                   VARCHAR(100),
  referral_type                  VARCHAR(100),
  referral_sts                   VARCHAR(50),
  start_dtm                      TIMESTAMP,
  end_dtm                        TIMESTAMP,
  delete_flg                     BOOLEAN,
  workflow_run_id                VARCHAR(50) not null,
  file_nm                        VARCHAR(1000),
  created_by                     VARCHAR(100) DEFAULT 'acp_system' NOT NULL,
  created_dt                     TIMESTAMP DEFAULT getdate()    NOT NULL,
  updated_by                     VARCHAR(100),
  updated_dt                     TIMESTAMP,
  primary key(edw_referral_sts_hist_key)
)
  SORTKEY
    (
     referral_id,
     reqst_method
    );
