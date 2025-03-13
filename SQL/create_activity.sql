CREATE TABLE "heartbeats" (
	"component" VARCHAR(64) NOT NULL,
    "datetime" TIMESTAMP NOT NULL,
)
;
COMMENT ON COLUMN "heartbeats"."component" IS '';
COMMENT ON COLUMN "heartbeats"."datetime" IS '';

ALTER TABLE "heartbeats" ADD PRIMARY KEY ("component");

CREATE TABLE "activity" (
	"activity_id" INTEGER NOT NULL,
    "component" VARCHAR(64) NOT NULL,	
	"datetime" TIMESTAMP NOT NULL,
	"severity" INTEGER NOT NULL,
	"message" TEXT NOT NULL,
    "send_alert" BOOLEAN DEFAULT FALSE,
    "alert_sent_datetime" TIMESTAMP DEFAULT NULL,
    "resolve_initials" VARCHAR(16) DEFAULT NULL,	
    "resolve_message" TEXT DEFAULT NULL,
    "resolve_datetime" TIMESTAMP DEFAULT NULL
)
;
COMMENT ON COLUMN "activity"."activity_id" IS '';
COMMENT ON COLUMN "activity"."component" IS '';
COMMENT ON COLUMN "activity"."datetime" IS '';
COMMENT ON COLUMN "activity"."severity" IS '';
COMMENT ON COLUMN "activity"."message" IS '';
COMMENT ON COLUMN "activity"."send_alert" IS '';
COMMENT ON COLUMN "activity"."alert_sent_datetime" IS '';
COMMENT ON COLUMN "activity"."resolve_initials" IS '';
COMMENT ON COLUMN "activity"."resolve_message" IS '';
COMMENT ON COLUMN "activity"."resolve_datetime" IS '';

ALTER TABLE "activity" ADD PRIMARY KEY ("activity_id");
CREATE SEQUENCE activity_id_seq;
ALTER TABLE activity ALTER COLUMN activity_id SET DEFAULT nextval('activity_id_seq');
CREATE INDEX idx_severity_datetime ON activity (severity, datetime);