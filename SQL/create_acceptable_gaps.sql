CREATE TABLE "acceptable_gaps" (
	"acceptable_gaps_id" INTEGER NOT NULL,
    "market" VARCHAR(64) NOT NULL,
    "start_datetime" TIMESTAMP NOT NULL,
    "end_datetime" TIMESTAMP NOT NULL,
    "description" TEXT DEFAULT NULL,
    "reoccur_day_of_week" INTEGER DEFAULT NULL,
    "reoccur_start_time" TIME DEFAULT NULL,
    "reoccur_end_time" TIME DEFAULT NULL
)
;
COMMENT ON COLUMN "acceptable_gaps"."acceptable_gaps_id" IS '';
COMMENT ON COLUMN "acceptable_gaps"."market" IS '';
COMMENT ON COLUMN "acceptable_gaps"."start_datetime" IS '';
COMMENT ON COLUMN "acceptable_gaps"."end_datetime" IS '';
COMMENT ON COLUMN "acceptable_gaps"."description" IS '';
COMMENT ON COLUMN "acceptable_gaps"."reoccur_day_of_week" IS '';
COMMENT ON COLUMN "acceptable_gaps"."reoccur_start_time" IS '';
COMMENT ON COLUMN "acceptable_gaps"."reoccur_end_time" IS '';

ALTER TABLE "acceptable_gaps" ADD PRIMARY KEY ("acceptable_gaps_id");
CREATE SEQUENCE acceptable_gaps_id_seq;
ALTER TABLE acceptable_gaps ALTER COLUMN acceptable_gaps_id SET DEFAULT nextval('acceptable_gaps_id_seq');
CREATE INDEX idx_market ON acceptable_gaps (market);