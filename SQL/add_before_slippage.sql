ALTER TABLE "trades" ADD "entry_price_before_slippage" DOUBLE PRECISION NOT NULL DEFAULT '0';
ALTER TABLE "trades" ADD "exit_price_before_slippage" DOUBLE PRECISION NOT NULL DEFAULT '0';
