CREATE TABLE "parser_config" (
    "source" VARCHAR(64) NOT NULL,
    "bar_type" VARCHAR(64) NOT NULL,	
	"start_index" INTEGER DEFAULT '0',
	"end_index" INTEGER DEFAULT '0',
	"open_index" INTEGER DEFAULT '0',
	"high_index" INTEGER DEFAULT '0',
	"low_index" INTEGER DEFAULT '0',
	"close_index" INTEGER DEFAULT '0',
	"volume_index" INTEGER DEFAULT '0',
	"calculate_forward_fill" BOOLEAN DEFAULT FALSE
)
;
COMMENT ON COLUMN "parser_config"."source" IS '';
COMMENT ON COLUMN "parser_config"."bar_type" IS '';
COMMENT ON COLUMN "parser_config"."start_index" IS '';
COMMENT ON COLUMN "parser_config"."end_index" IS '';
COMMENT ON COLUMN "parser_config"."open_index" IS '';
COMMENT ON COLUMN "parser_config"."high_index" IS '';
COMMENT ON COLUMN "parser_config"."low_index" IS '';
COMMENT ON COLUMN "parser_config"."close_index" IS '';
COMMENT ON COLUMN "parser_config"."volume_index" IS '';
COMMENT ON COLUMN "parser_config"."calculate_forward_fill" IS '';

ALTER TABLE "parser_config" ADD PRIMARY KEY ("source", "bar_type");

ALTER TABLE "parser_config" ADD "name_start" VARCHAR(32) NULL;
ALTER TABLE "parser_config" ADD "name_end" VARCHAR(32) NULL;
ALTER TABLE "parser_config" ADD "name_open" VARCHAR(32) NULL;
ALTER TABLE "parser_config" ADD "name_high" VARCHAR(32) NULL;
ALTER TABLE "parser_config" ADD "name_low" VARCHAR(32) NULL;
ALTER TABLE "parser_config" ADD "name_close" VARCHAR(32) NULL;
ALTER TABLE "parser_config" ADD "name_volume" VARCHAR(32) NULL;
