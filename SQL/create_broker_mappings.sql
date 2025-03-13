CREATE TABLE "broker_mappings" (
    "broker_mappings_id" INTEGER NOT NULL,
	"market" VARCHAR(64) NOT NULL,
    "broker" VARCHAR(64) NOT NULL,
    "broker_market" VARCHAR(64) NOT NULL
)
;
COMMENT ON COLUMN "broker_mappings"."market" IS '';
COMMENT ON COLUMN "broker_mappings"."broker" IS '';
COMMENT ON COLUMN "broker_mappings"."broker_market" IS '';

ALTER TABLE "broker_mappings" ADD PRIMARY KEY ("broker_mappings_id");

CREATE SEQUENCE broker_mapping_id_seq;
ALTER TABLE broker_mappings ALTER COLUMN broker_mappings_id SET DEFAULT nextval('broker_mapping_id_seq');

CREATE INDEX idx_broker_mappings_market_broker ON broker_mappings(market, broker);

ALTER TABLE broker_mappings ADD CONSTRAINT uq_broker_mappings_market_broker UNIQUE (market, broker);