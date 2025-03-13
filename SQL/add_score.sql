ALTER TABLE "scores" ADD "score" DOUBLE PRECISION NOT NULL DEFAULT '0';
ALTER TABLE "scores" ADD "edge_better_than_random0" DOUBLE PRECISION NOT NULL DEFAULT '0';

ALTER TABLE "scores"
	RENAME COLUMN "edge0" TO "cost_percentage0";
ALTER TABLE "scores"
	ALTER COLUMN "cost_percentage0" TYPE DOUBLE PRECISION,
	ALTER COLUMN "cost_percentage0" SET NOT NULL,
	ALTER COLUMN "cost_percentage0" SET DEFAULT '0';

ALTER TABLE "scores"
	DROP COLUMN "edge4";

ALTER TABLE "scores"
	DROP COLUMN "edge8";

ALTER TABLE "scores"
	DROP COLUMN "edge13";

ALTER TABLE "scores"
	DROP COLUMN "edge26";

ALTER TABLE "scores"
	DROP COLUMN "edge52";

ALTER TABLE "scores"
	DROP COLUMN "edge104";

ALTER TABLE "scores"
	DROP COLUMN "edge156";

ALTER TABLE "scores"
	DROP COLUMN "edge208";

ALTER TABLE "scores"
	DROP COLUMN "edge520";
