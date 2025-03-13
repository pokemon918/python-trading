ALTER TABLE "markets" ADD "top_of_book" INTEGER NOT NULL DEFAULT '0';
ALTER TABLE "markets" ADD "enabled_risk_events" BOOLEAN DEFAULT TRUE;
ALTER TABLE "markets" ADD "break_start_time" TIME NULL;
ALTER TABLE "markets" ADD "break_end_time" TIME NULL;
ALTER TABLE "markets" ADD "instrument_type" VARCHAR(32) NULL;
ALTER TABLE "markets" ADD "external_ib" VARCHAR(32) NULL;
ALTER TABLE "markets" ADD "external_first_rate_data" VARCHAR(32) NULL;
ALTER TABLE "markets" ADD "slippage" DOUBLE PRECISION NOT NULL DEFAULT '0';