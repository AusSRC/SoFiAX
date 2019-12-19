\connect sofiadb

CREATE EXTENSION postgis;



CREATE TABLE "Observation" (
  "id" BIGSERIAL PRIMARY KEY,
  "name" varchar NOT NULL,
  unique ("name")
);

CREATE TABLE "Run" (
  "id" BIGSERIAL PRIMARY KEY,
  "name" varchar NOT NULL,
  "sanity_thresholds" jsonb NOT NULL,
  "obs_id" bigint NOT NULL,
   unique ("name", "sanity_thresholds", "obs_id")
);

CREATE TABLE "Instance" (
  "id" BIGSERIAL PRIMARY KEY,
  "run_id" bigint NOT NULL,
  "filename" varchar NOT NULL,
  "boundary" integer[] NOT NULL,
  "run_date" timestamp without time zone NOT NULL,
  "flag_log" bytea,
  "reliability_plot" bytea,
  "log" bytea,
  "parameters" jsonb NOT NULL,
  unique ("run_id", "filename", "boundary")
);

CREATE TABLE "Detection" (
  "id" BIGSERIAL PRIMARY KEY,
  "instance_id" bigint NOT NULL,
  "run_id" bigint NOT NULL,
  "name" varchar,
  "x" double precision NOT NULL,
  "y" double precision NOT NULL ,
  "z" double precision NOT NULL,
  "x_min" numeric,
  "x_max" numeric,
  "y_min" numeric,
  "y_max" numeric,
  "z_min" numeric,
  "z_max" numeric,
  "n_pix" numeric,
  "f_min" double precision,
  "f_max" double precision,
  "f_sum" double precision,
  "rel" double precision,
  "rms" double precision,
  "w20" double precision,
  "w50" double precision,
  "ell_maj" double precision,
  "ell_min" double precision,
  "ell_pa" double precision,
  "ell3s_maj" double precision,
  "ell3s_min" double precision,
  "ell3s_ps" double precision,
  "err_x" double precision NOT NULL,
  "err_y" double precision NOT NULL,
  "err_z" double precision NOT NULL,
  "err_f_sum" double precision,
  "ra" double precision NOT NULL,
  "dec" double precision NOT NULL,
  "freq" double precision NOT NULL,
  "flag" int,
  "unresolved" boolean DEFAULT False NOT NULL,
  unique ("ra", "dec", "freq", "instance_id", "run_id")
);


CREATE TABLE "Products" (
  "id" BIGSERIAL PRIMARY KEY,
  "detection_id" bigint NOT NULL,
  "cube" bytea,
  "mask" bytea,
  "moment0" bytea,
  "moment1" bytea,
  "moment2" bytea,
  "channels" bytea,
  "spectrum" bytea,
  unique ("detection_id")
);

ALTER TABLE "Run" ADD FOREIGN KEY ("obs_id") REFERENCES "Observation" ("id") ON DELETE CASCADE;
ALTER TABLE "Instance" ADD FOREIGN KEY ("run_id") REFERENCES "Run" ("id") ON DELETE CASCADE;
ALTER TABLE "Detection" ADD FOREIGN KEY ("instance_id") REFERENCES "Instance" ("id") ON DELETE CASCADE;
ALTER TABLE "Detection" ADD FOREIGN KEY ("run_id") REFERENCES "Run" ("id") ON DELETE CASCADE;
ALTER TABLE "Products" ADD FOREIGN KEY ("detection_id") REFERENCES "Detection" ("id") ON DELETE CASCADE;

ALTER TABLE "Observation" OWNER TO "sofia_user";
ALTER TABLE "Run" OWNER TO "sofia_user";
ALTER TABLE "Instance" OWNER TO "sofia_user";
ALTER TABLE "Detection" OWNER TO "sofia_user";
ALTER TABLE "Products" OWNER TO "sofia_user";