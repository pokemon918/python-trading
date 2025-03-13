CREATE database strategies;

CREATE TABLE "strategies" (
	"strategy_id" INTEGER NOT NULL,
	"run_id" INTEGER NOT NULL,
	"creation_datetime" TIMESTAMP NOT NULL,
	"optimisation_date" TIMESTAMP NOT NULL,
	"market" VARCHAR(64) NOT NULL,
	"json" TEXT NOT NULL
)
;
COMMENT ON COLUMN "strategies"."strategy_id" IS '';
COMMENT ON COLUMN "strategies"."run_id" IS '';
COMMENT ON COLUMN "strategies"."creation_datetime" IS '';
COMMENT ON COLUMN "strategies"."optimisation_datetime" IS '';
COMMENT ON COLUMN "strategies"."market" IS '';
COMMENT ON COLUMN "strategies"."json" IS '';

ALTER TABLE "strategies" ADD PRIMARY KEY ("strategy_id");
CREATE SEQUENCE strategy_id_seq;
ALTER TABLE strategies ALTER COLUMN strategy_id SET DEFAULT nextval('strategy_id_seq');

CREATE TABLE "scores" (
	"strategy_id" INTEGER NOT NULL,
	"optimisation_date" TIMESTAMP NOT NULL,
	"min_sharpes0" DOUBLE PRECISION NOT NULL,
	"min_sharpes52" DOUBLE PRECISION NOT NULL,
	"tradable_weeks_rate0" DOUBLE PRECISION NOT NULL
)
;
COMMENT ON COLUMN "scores"."strategy_id" IS '';
COMMENT ON COLUMN "scores"."optimisation_date" IS '';
COMMENT ON COLUMN "scores"."min_sharpes0" IS '';
COMMENT ON COLUMN "scores"."min_sharpes52" IS '';
COMMENT ON COLUMN "scores"."tradable_weeks_rate0" IS '';

ALTER TABLE "scores" ADD PRIMARY KEY ("strategy_id", "optimisation_date");

CREATE TABLE "runs" (
	"run_id" INTEGER NOT NULL,
	"strategy_id" INTEGER NULL,
	"config_filename" VARCHAR(128) NOT NULL,
	"config" TEXT NOT NULL,
	"version" VARCHAR(128) NOT NULL,
	"start_datetime" TIMESTAMP NOT NULL,
	"end_datetime" TIMESTAMP NULL DEFAULT NULL,
	"market" VARCHAR(64) NOT NULL,
	"optimisation_date" TIMESTAMP NOT NULL
)
;
COMMENT ON COLUMN "runs"."run_id" IS '';
COMMENT ON COLUMN "runs"."strategy_id" IS '';
COMMENT ON COLUMN "runs"."config_filename" IS '';
COMMENT ON COLUMN "runs"."config" IS '';
COMMENT ON COLUMN "runs"."version" IS '';
COMMENT ON COLUMN "runs"."start_datetime" IS '';
COMMENT ON COLUMN "runs"."end_datetime" IS '';
COMMENT ON COLUMN "runs"."market" IS '';
COMMENT ON COLUMN "runs"."optimisation_date" IS '';

ALTER TABLE "runs" ADD PRIMARY KEY ("run_id");
CREATE SEQUENCE run_id_seq;
ALTER TABLE runs ALTER COLUMN run_id SET DEFAULT nextval('run_id_seq');

CREATE TABLE "portfolios" (
	"portfolio_name" VARCHAR(128) NOT NULL,
	"optimisation_date" TIMESTAMP NOT NULL,
	"strategy_id" INTEGER NOT NULL,
	"weighting" INTEGER NOT NULL
)
;
COMMENT ON COLUMN "portfolios"."portfolio_name" IS '';
COMMENT ON COLUMN "portfolios"."optimisation_date" IS '';
COMMENT ON COLUMN "portfolios"."strategy_id" IS '';
COMMENT ON COLUMN "portfolios"."weighting" IS '';

ALTER TABLE "portfolios" ADD PRIMARY KEY ("portfolio_name", "optimisation_date", "strategy_id");

