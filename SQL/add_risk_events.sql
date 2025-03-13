CREATE TABLE "risk_event_calendar" (
	"event_id" INTEGER NOT NULL,
    "event_date" TIMESTAMP NOT NULL
)
;
COMMENT ON COLUMN "risk_event_calendar"."event_id" IS '';
COMMENT ON COLUMN "risk_event_calendar"."event_date" IS '';

ALTER TABLE "risk_event_calendar" ADD PRIMARY KEY ("event_id", "event_date");

CREATE TABLE "risk_event_types" (
	"event_id" INTEGER NOT NULL,
    "event_code" VARCHAR(64) NOT NULL,    
    "stop_before_event" INTEGER NOT NULL,
    "resume_after_event" INTEGER NOT NULL
)
;
COMMENT ON COLUMN "risk_event_types"."event_id" IS '';
COMMENT ON COLUMN "risk_event_types"."event_code" IS '';
COMMENT ON COLUMN "risk_event_types"."stop_before_event" IS '';
COMMENT ON COLUMN "risk_event_types"."resume_after_event" IS '';

ALTER TABLE "risk_event_types" ADD PRIMARY KEY ("event_id");

CREATE TABLE "risk_event_markets" (
	"event_id" INTEGER NOT NULL,
    "market" VARCHAR(64) NOT NULL
)
;
COMMENT ON COLUMN "risk_event_markets"."event_id" IS '';
COMMENT ON COLUMN "risk_event_markets"."market" IS '';

ALTER TABLE "risk_event_markets" ADD PRIMARY KEY ("event_id","market");
