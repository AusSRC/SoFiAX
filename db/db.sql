\connect sofiadb

CREATE EXTENSION postgis;
CREATE EXTENSION pg_sphere;

CREATE SCHEMA "wallaby" AUTHORIZATION "admin";

CREATE TABLE wallaby.run (
  "id" BIGSERIAL PRIMARY KEY,
  "name" varchar NOT NULL,
  "sanity_thresholds" jsonb NOT NULL,
   unique ("name", "sanity_thresholds")
);

CREATE TABLE wallaby.instance (
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

CREATE TABLE wallaby.detection (
  "id" BIGSERIAL PRIMARY KEY,
  "instance_id" bigint NOT NULL,
  "run_id" bigint NOT NULL,
  "name" varchar NOT NULL,
  "access_url" varchar NOT NULL,
  "access_format" varchar DEFAULT 'application/x-votable+xml;content=datalink' NOT NULL,
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
  unique ("name", "x", "y", "z", "x_min", "x_max", "y_min", "y_max", "z_min", "z_max", "n_pix", "f_min", "f_max",
  "f_sum", "instance_id", "run_id")
);


CREATE TABLE wallaby.products (
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

ALTER TABLE wallaby.instance ADD FOREIGN KEY ("run_id") REFERENCES wallaby.run ("id") ON DELETE CASCADE;
ALTER TABLE wallaby.detection ADD FOREIGN KEY ("instance_id") REFERENCES wallaby.instance ("id") ON DELETE CASCADE;
ALTER TABLE wallaby.detection ADD FOREIGN KEY ("run_id") REFERENCES wallaby.run ("id") ON DELETE CASCADE;
ALTER TABLE wallaby.products ADD FOREIGN KEY ("detection_id") REFERENCES wallaby.detection ("id") ON DELETE CASCADE;

ALTER TABLE wallaby.run OWNER TO "admin";
ALTER TABLE wallaby.instance OWNER TO "admin";
ALTER TABLE wallaby.detection OWNER TO "admin";
ALTER TABLE wallaby.products OWNER TO "admin";

GRANT ALL PRIVILEGES ON DATABASE sofiadb TO "admin";
GRANT ALL PRIVILEGES ON DATABASE sofiadb TO "gavoadmin";
GRANT ALL PRIVILEGES ON DATABASE sofiadb TO "gavo";

GRANT CONNECT ON DATABASE sofiadb TO "wallaby_user";
GRANT USAGE ON SCHEMA "wallaby" TO "wallaby_user";
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE wallaby.instance, wallaby.detection, wallaby.run, wallaby.products TO "wallaby_user";
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA wallaby TO "wallaby_user";

GRANT CONNECT ON DATABASE sofiadb TO "gavoadmin";
GRANT USAGE ON SCHEMA "wallaby" TO "gavoadmin";
GRANT SELECT ON TABLE wallaby.instance, wallaby.detection, wallaby.run, wallaby.products TO "gavoadmin";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA wallaby TO "gavoadmin";

GRANT CONNECT ON DATABASE sofiadb TO "gavo";
GRANT USAGE ON SCHEMA "wallaby" TO "gavo";
GRANT SELECT ON TABLE wallaby.instance, wallaby.detection, wallaby.run, wallaby.products TO "gavo";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA wallaby TO "gavo";

GRANT CONNECT ON DATABASE sofiadb TO "untrusted";
GRANT USAGE ON SCHEMA "wallaby" TO "untrusted";
GRANT SELECT ON TABLE wallaby.instance, wallaby.detection, wallaby.run, wallaby.products TO "untrusted";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA wallaby TO "untrusted";