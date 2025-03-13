ALTER TABLE "runs"
	ADD "population_size" INTEGER NULL;
COMMENT ON COLUMN "runs"."population_size" IS '';
ALTER TABLE "runs"
	ADD "generations" INTEGER NULL;
COMMENT ON COLUMN "runs"."generations" IS '';