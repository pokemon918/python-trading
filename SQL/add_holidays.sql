CREATE TABLE "holidays" (
	"start" TIMESTAMP NOT NULL,
	"end" TIMESTAMP NOT NULL
)
;
COMMENT ON COLUMN "holidays"."start" IS '';
COMMENT ON COLUMN "holidays"."end" IS '';

ALTER TABLE "holidays" ADD PRIMARY KEY ("start");

ALTER TABLE "holidays" ADD "name" VARCHAR(64) NOT NULL DEFAULT '';
