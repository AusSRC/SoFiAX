\connect sofiadb

CREATE TABLE "Observation" (
  "id" SERIAL PRIMARY KEY,
  "name" varchar NOT NULL,
  "datacube" varchar NOT NULL,
  unique ("name", "datacube")
);

CREATE TABLE "Run" (
  "id" SERIAL PRIMARY KEY,
  "run_date" timestamp without time zone,
  "sofia_boundary" polygon,
  "sofia_parameters" bytea,
  "sofia_flag_log" bytea,
  "sofia_reliability_plot" bytea,
  "sofia_log" bytea,
  "obs_id" int
);

CREATE TABLE "Detection" (
  "id" SERIAL PRIMARY KEY,
  "run_id" int,
  "name" varchar,
  "x" double precision,
  "y" double precision,
  "z" double precision,
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
  "err_x" double precision,
  "err_y" double precision,
  "err_z" double precision,
  "err_f_sum" double precision,
  "ra" double precision,
  "dec" double precision,
  "freq" double precision
);

CREATE TABLE "Products" (
  "id" SERIAL PRIMARY KEY,
  "detection_id" int,
  "cube" bytea,
  "mask" bytea,
  "moment0" bytea,
  "moment1" bytea,
  "moment2" bytea,
  "channels" bytea,
  "spectrum" bytea
);

ALTER TABLE "Run" ADD FOREIGN KEY ("obs_id") REFERENCES "Observation" ("id");

ALTER TABLE "Detection" ADD FOREIGN KEY ("run_id") REFERENCES "Run" ("id");

ALTER TABLE "Products" ADD FOREIGN KEY ("detection_id") REFERENCES "Detection" ("id");
