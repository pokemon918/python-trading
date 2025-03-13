CREATE TABLE "predictions" (
	"model_name" VARCHAR(128) NOT NULL,
	"optimisation_date" TIMESTAMP NOT NULL,
	"strategy_id" INTEGER NOT NULL,
	"prediction" DOUBLE PRECISION NOT NULL
)
;
COMMENT ON COLUMN "predictions"."model_name" IS '';
COMMENT ON COLUMN "predictions"."optimisation_date" IS '';
COMMENT ON COLUMN "predictions"."strategy_id" IS '';
COMMENT ON COLUMN "predictions"."prediction" IS '';

ALTER TABLE "predictions" ADD PRIMARY KEY ("model_name", "optimisation_date", "strategy_id");