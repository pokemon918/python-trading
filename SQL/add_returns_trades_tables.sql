CREATE TABLE "returns" (
	"strategy_id" INTEGER NOT NULL,
	"datetime" TIMESTAMP NOT NULL,
	"return" DOUBLE PRECISION NOT NULL
)
;
COMMENT ON COLUMN "returns"."strategy_id" IS '';
COMMENT ON COLUMN "returns"."datetime" IS '';
COMMENT ON COLUMN "returns"."return" IS '';

ALTER TABLE "returns" ADD PRIMARY KEY ("strategy_id", "datetime");

CREATE TABLE "trades" (
	"strategy_id" INTEGER NOT NULL,
	"direction" VARCHAR(16) NOT NULL,
	"entry_datetime" TIMESTAMP NOT NULL,
	"exit_datetime" TIMESTAMP NOT NULL,	
	"entry_price" DOUBLE PRECISION NOT NULL,
	"exit_price" DOUBLE PRECISION NOT NULL,
	"return" DOUBLE PRECISION NOT NULL
)
;
COMMENT ON COLUMN "trades"."strategy_id" IS '';
COMMENT ON COLUMN "trades"."direction" IS '';
COMMENT ON COLUMN "trades"."entry_datetime" IS '';
COMMENT ON COLUMN "trades"."exit_datetime" IS '';
COMMENT ON COLUMN "trades"."entry_price" IS '';
COMMENT ON COLUMN "trades"."exit_price" IS '';
COMMENT ON COLUMN "trades"."return" IS '';

ALTER TABLE "trades" ADD PRIMARY KEY ("strategy_id", "entry_datetime");

