DROP TABLE IF EXISTS acp_edw.edw.referral_recipient;

CREATE TABLE acp_edw.edw.referral_recipient(
  referral_id                    VARCHAR(50) NOT NULL DISTKEY,
  reqst_method                   VARCHAR(100),
  referral_type                  VARCHAR(100),
  recipient_id                   VARCHAR(100),
  recipient_type                 VARCHAR(100),
  last_upd_dtm                   TIMESTAMP   NOT NULL,
  workflow_run_id                VARCHAR(50) not null,
  file_nm                        VARCHAR(1000),
  created_by                     VARCHAR(100) DEFAULT 'acp_system' NOT NULL,
  created_dt                     TIMESTAMP DEFAULT getdate()       NOT NULL,
  updated_by                     VARCHAR(100),
  updated_dt                     TIMESTAMP
)
  SORTKEY
    (
     last_upd_dtm
    );
