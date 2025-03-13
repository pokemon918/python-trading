CREATE TABLE "live_orders" (
	"account" VARCHAR(32) NOT NULL,
	"broker_id" VARCHAR(32) NOT NULL,
    "parent_id" VARCHAR(32) NOT NULL,
    "order_status" VARCHAR(32) NOT NULL,
    "order_type" VARCHAR(32) NOT NULL,
	"direction" VARCHAR(16) NOT NULL,
	"market" VARCHAR(64) NOT NULL,
	"symbol" VARCHAR(64) NOT NULL,
	"strategy_id" INTEGER NOT NULL,
	"submitted_datetime" TIMESTAMP NOT NULL,
	"requested_price" DOUBLE PRECISION NOT NULL,
	"requested_size" INTEGER NOT NULL,
	"average_fill_price" DOUBLE PRECISION NULL,
	"filled_size" INTEGER NULL,
	"filled_datetime" TIMESTAMP NULL,
	"return" DOUBLE PRECISION NOT NULL DEFAULT 0.0
)
;
COMMENT ON COLUMN "live_orders"."account" IS '';
COMMENT ON COLUMN "live_orders"."broker_id" IS '';
COMMENT ON COLUMN "live_orders"."parent_id" IS '';
COMMENT ON COLUMN "live_orders"."direction" IS '';
COMMENT ON COLUMN "live_orders"."market" IS '';
COMMENT ON COLUMN "live_orders"."symbol" IS '';
COMMENT ON COLUMN "live_orders"."strategy_id" IS '';
COMMENT ON COLUMN "live_orders"."submitted_datetime" IS '';
COMMENT ON COLUMN "live_orders"."requested_price" IS '';
COMMENT ON COLUMN "live_orders"."requested_size" IS '';
COMMENT ON COLUMN "live_orders"."average_fill_price" IS '';
COMMENT ON COLUMN "live_orders"."filled_size" IS '';
COMMENT ON COLUMN "live_orders"."order_type" IS '';
COMMENT ON COLUMN "live_orders"."filled_datetime" IS '';
COMMENT ON COLUMN "live_orders"."order_status" IS '';
COMMENT ON COLUMN "live_orders"."return" IS '';

ALTER TABLE "live_orders" ADD PRIMARY KEY ("account", "broker_id");

ALTER TABLE "live_orders" ADD "timed_exit" TIMESTAMP NULL;
COMMENT ON COLUMN "live_orders"."timed_exit" IS '';
