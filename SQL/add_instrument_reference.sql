CREATE TABLE "instrument_reference" (
	"market" VARCHAR(64) NOT NULL,
	"exchange" VARCHAR(64) NOT NULL,
	"symbol" VARCHAR(32) NOT NULL,
	"expiry" TIMESTAMP NOT NULL,
	"tick_size" DOUBLE PRECISION NOT NULL,
	"start_date" TIMESTAMP NULL,
	"end_date" TIMESTAMP NULL
)
;
COMMENT ON COLUMN "instrument_reference"."market" IS '';
COMMENT ON COLUMN "instrument_reference"."exchange" IS '';
COMMENT ON COLUMN "instrument_reference"."symbol" IS '';
COMMENT ON COLUMN "instrument_reference"."expiry" IS '';
COMMENT ON COLUMN "instrument_reference"."tick_size" IS '';
COMMENT ON COLUMN "instrument_reference"."start_date" IS '';
COMMENT ON COLUMN "instrument_reference"."end_date" IS '';

ALTER TABLE "instrument_reference" ADD PRIMARY KEY ("market", "symbol");

CREATE TABLE "main_contract_months" (
	"market" VARCHAR(64) NOT NULL,
	"main_month" VARCHAR(8) NOT NULL
)
;
COMMENT ON COLUMN "main_contract_months"."market" IS '';
COMMENT ON COLUMN "main_contract_months"."main_month" IS '';

ALTER TABLE "main_contract_months" ADD PRIMARY KEY ("market", "main_month");