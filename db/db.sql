\connect sofiadb

CREATE EXTENSION postgis;



CREATE TABLE "Run" (
  "id" BIGSERIAL PRIMARY KEY,
  "name" varchar NOT NULL,
  "sanity_thresholds" jsonb NOT NULL,
   unique ("name", "sanity_thresholds")
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
  "version" varchar,
  "return_code" integer,
  "stdout" bytea,
  "stderr" bytea,
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
  "x_min" numeric NOT NULL,
  "x_max" numeric NOT NULL,
  "y_min" numeric NOT NULL,
  "y_max" numeric NOT NULL,
  "z_min" numeric NOT NULL,
  "z_max" numeric NOT NULL,
  "n_pix" numeric NOT NULL,
  "f_min" double precision NOT NULL,
  "f_max" double precision NOT NULL,
  "f_sum" double precision NOT NULL,
  "rel" double precision,
  "rms" double precision NOT NULL,
  "w20" double precision NOT NULL,
  "w50" double precision NOT NULL,
  "ell_maj" double precision NOT NULL,
  "ell_min" double precision NOT NULL,
  "ell_pa" double precision NOT NULL,
  "ell3s_maj" double precision NOT NULL,
  "ell3s_min" double precision NOT NULL,
  "ell3s_pa" double precision NOT NULL,
  "kin_pa" double precision,
  "ra" double precision,
  "dec" double precision,
  "l" double precision,
  "b" double precision,
  "v_rad" double precision,
  "v_opt" double precision,
  "v_app" double precision,
  "err_x" double precision NOT NULL,
  "err_y" double precision NOT NULL,
  "err_z" double precision NOT NULL,
  "err_f_sum" double precision NOT NULL,
  "freq" double precision,
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

ALTER TABLE "Instance" ADD FOREIGN KEY ("run_id") REFERENCES "Run" ("id") ON DELETE CASCADE;
ALTER TABLE "Detection" ADD FOREIGN KEY ("instance_id") REFERENCES "Instance" ("id") ON DELETE CASCADE;
ALTER TABLE "Detection" ADD FOREIGN KEY ("run_id") REFERENCES "Run" ("id") ON DELETE CASCADE;
ALTER TABLE "Products" ADD FOREIGN KEY ("detection_id") REFERENCES "Detection" ("id") ON DELETE CASCADE;

ALTER TABLE "Run" OWNER TO "sofia_user";
ALTER TABLE "Instance" OWNER TO "sofia_user";
ALTER TABLE "Detection" OWNER TO "sofia_user";
ALTER TABLE "Products" OWNER TO "sofia_user";