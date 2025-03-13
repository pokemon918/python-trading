alter table "gc" ALTER COLUMN "volume" SET DATA TYPE real;
alter table "gc" alter column "volume" set default 0.000001;
update gc set volume = 0.000001 where volume = 0;

alter table "cl" ALTER COLUMN "volume" SET DATA TYPE real;
alter table "cl" alter column "volume" set default 0.000001;
update cl set volume = 0.000001 where volume = 0;

alter table "es" ALTER COLUMN "volume" SET DATA TYPE real;
alter table "es" alter column "volume" set default 0.000001;
update es set volume = 0.000001 where volume = 0;

alter table "eu" ALTER COLUMN "volume" SET DATA TYPE real;
alter table "eu" alter column "volume" set default 0.000001;
update eu set volume = 0.000001 where volume = 0;

alter table "hg" ALTER COLUMN "volume" SET DATA TYPE real;
alter table "hg" alter column "volume" set default 0.000001;
update hg set volume = 0.000001 where volume = 0;

alter table "nq" ALTER COLUMN "volume" SET DATA TYPE real;
alter table "nq" alter column "volume" set default 0.000001;
update nq set volume = 0.000001 where volume = 0;
