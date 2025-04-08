CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE public.clients (
	id uuid DEFAULT uuid_generate_v4() NOT NULL,
	"Name" varchar(200) NOT NULL,
	CONSTRAINT clients_pk PRIMARY KEY (id)
);

CREATE TABLE public.models (
	id uuid DEFAULT gen_random_uuid() NOT NULL,
	model_name text NOT NULL,
	"version" text NOT NULL,
	training_date timestamp NULL,
	accuracy float8 NULL,
	CONSTRAINT models_pkey PRIMARY KEY (id),
	CONSTRAINT models_version_key UNIQUE (version)
);


CREATE TABLE public.requests_log (
	id uuid DEFAULT gen_random_uuid() NOT NULL,
	"timestamp" timestamp DEFAULT now() NULL,
	endpoint text NULL,
	request_payload jsonb NULL,
	response_payload jsonb NULL,
	response_time_ms int4 NULL,
	model_version text NULL,
	status_code int4 NULL,
	client_id uuid NULL,
	CONSTRAINT requests_log_pkey PRIMARY KEY (id),
	CONSTRAINT requests_log_clients_fk FOREIGN KEY (client_id) REFERENCES public.clients(id)
);


CREATE TABLE public.predictions (
	id uuid DEFAULT gen_random_uuid() NOT NULL,
	request_id uuid NULL,
	model_version text NULL,
	input_data jsonb NULL,
	prediction_result jsonb NULL,
	confidence float8 NULL,
	created_at timestamp DEFAULT now() NULL,
	CONSTRAINT predictions_pkey PRIMARY KEY (id),
	CONSTRAINT predictions_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.requests_log(id) ON DELETE CASCADE
);

CREATE TABLE public.prompts (
	id uuid DEFAULT gen_random_uuid() NOT NULL,
	prompt_text text NOT NULL,
	model_version text NULL,
	created_at timestamp DEFAULT now() NULL,
	CONSTRAINT prompts_pkey PRIMARY KEY (id)
);

CREATE TABLE public.api_keys (
	id uuid DEFAULT uuid_generate_v4() NOT NULL,
	api_key text NOT NULL,
	created_at timestamp DEFAULT now() NULL,
	last_used timestamp NULL,
	client_id uuid NOT NULL,
	CONSTRAINT api_keys_api_key_key UNIQUE (api_key),
	CONSTRAINT api_keys_pkey PRIMARY KEY (id),
	CONSTRAINT api_keys_clients_fk FOREIGN KEY (client_id) REFERENCES public.clients(id)
);

