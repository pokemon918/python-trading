ALTER TABLE "runs" ADD "improver_id" INTEGER DEFAULT '0';
ALTER TABLE "runs" ADD "improver_inital_score" DOUBLE PRECISION NOT NULL DEFAULT '0';
ALTER TABLE "runs" ADD "improver_final_score" DOUBLE PRECISION NOT NULL DEFAULT '0';
