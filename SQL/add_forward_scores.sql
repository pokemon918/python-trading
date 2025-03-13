ALTER TABLE "scores" ADD "forward_week_trade_win_count" DOUBLE PRECISION NOT NULL DEFAULT '0';
ALTER TABLE "scores" ADD "forward_week_average_win" DOUBLE PRECISION NOT NULL DEFAULT '0';
ALTER TABLE "scores" ADD "forward_week_average_loss" DOUBLE PRECISION NOT NULL DEFAULT '0';
ALTER TABLE "scores" ADD "weeks_after_optimisation" DOUBLE PRECISION NOT NULL DEFAULT '0';
