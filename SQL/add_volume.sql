ALTER TABLE "gc" ADD "volume" INTEGER NULL DEFAULT '0';
COMMENT ON COLUMN "gc"."volume" IS '';

ALTER TABLE "cl" ADD "volume" INTEGER NULL DEFAULT '0';
COMMENT ON COLUMN "cl"."volume" IS '';

ALTER TABLE "es" ADD "volume" INTEGER NULL DEFAULT '0';
COMMENT ON COLUMN "es"."volume" IS '';

ALTER TABLE "eu" ADD "volume" INTEGER NULL DEFAULT '0';
COMMENT ON COLUMN "eu"."volume" IS '';

ALTER TABLE "hg" ADD "volume" INTEGER NULL DEFAULT '0';
COMMENT ON COLUMN "hg"."volume" IS '';

ALTER TABLE "nq" ADD "volume" INTEGER NULL DEFAULT '0';
COMMENT ON COLUMN "nq"."volume" IS '';