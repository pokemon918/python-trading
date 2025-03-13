CREATE TABLE "broker_connections" (
    "broker_connection_id" INTEGER NOT NULL,
	"market" VARCHAR(64) NOT NULL,
    "broker" VARCHAR(64) NOT NULL    
)
;
COMMENT ON COLUMN "broker_connections"."market" IS '';
COMMENT ON COLUMN "broker_connections"."broker" IS '';

ALTER TABLE "broker_connections" ADD PRIMARY KEY ("broker_connection_id");

CREATE SEQUENCE broker_connection_id_seq;
ALTER TABLE broker_connections ALTER COLUMN broker_connection_id SET DEFAULT nextval('broker_connection_id_seq');

CREATE INDEX idx_broker_connections_market_broker ON broker_mappings(market, broker);

ALTER TABLE broker_connections ADD CONSTRAINT uq_broker_connections_market_broker UNIQUE (market, broker);