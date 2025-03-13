CREATE TABLE "configs" (
    "config_type" VARCHAR(128) NOT NULL,
	"datetime" TIMESTAMP NOT NULL,
	"config" TEXT NOT NULL
)
;
COMMENT ON COLUMN "configs"."config_type" IS '';
COMMENT ON COLUMN "configs"."datetime" IS '';
COMMENT ON COLUMN "configs"."config" IS '';

ALTER TABLE "configs" ADD PRIMARY KEY ("config_type", "datetime");