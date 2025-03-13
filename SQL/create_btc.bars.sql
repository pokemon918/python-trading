CREATE TABLE "btc" (
	"size" INTEGER NOT NULL,
	"symbol" VARCHAR(8) NOT NULL,
	"start" TIMESTAMP NOT NULL,
	"end" TIMESTAMP NOT NULL,
	"open" NUMERIC(18,9) NOT NULL,
	"high" NUMERIC(18,9) NOT NULL,
	"low" NUMERIC(18,9) NOT NULL,
	"close" NUMERIC(18,9) NOT NULL,
	"firstticktimestamp" VARCHAR(250) NULL DEFAULT NULL,
	"lastticktimestamp" VARCHAR(250) NULL DEFAULT NULL,
	"sendtofeedmanagertimestamp" TIMESTAMP NULL DEFAULT NULL,
	"id" BIGINT NOT NULL
)
;
COMMENT ON COLUMN "btc"."size" IS '';
COMMENT ON COLUMN "btc"."symbol" IS '';
COMMENT ON COLUMN "btc"."start" IS '';
COMMENT ON COLUMN "btc"."end" IS '';
COMMENT ON COLUMN "btc"."open" IS '';
COMMENT ON COLUMN "btc"."high" IS '';
COMMENT ON COLUMN "btc"."low" IS '';
COMMENT ON COLUMN "btc"."close" IS '';
COMMENT ON COLUMN "btc"."firstticktimestamp" IS '';
COMMENT ON COLUMN "btc"."lastticktimestamp" IS '';
COMMENT ON COLUMN "btc"."sendtofeedmanagertimestamp" IS '';
COMMENT ON COLUMN "btc"."id" IS '';

ALTER TABLE "btc" ADD "volume" real DEFAULT 0.000001;
COMMENT ON COLUMN "btc"."volume" IS '';

CREATE SEQUENCE btc_id_seq;
ALTER TABLE btc ALTER COLUMN id SET DEFAULT nextval('btc_id_seq');
ALTER TABLE "btc" ADD PRIMARY KEY ("id");
CREATE INDEX "btc_size_start_end" ON btc ("size", "start", "end");

-- No btc_size_symbol_start_end index because the symbol is fixed