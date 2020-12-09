#!/bin/bash

#create DB, schema and tables for setup

PGPASSWORD=password psql \
    -h 127.0.0.1 -U postgres \
    -c "CREATE DATABASE example"


PGPASSWORD=password psql \
    -h 127.0.0.1 -d example -U postgres \
    -c "CREATE schema simple"


PGPASSWORD=password psql \
    -h 127.0.0.1 -d example -U postgres \
    -c "CREATE TABLE simple.customer
            (
                id bigint NOT NULL,
                description text COLLATE pg_catalog."default",
                CONSTRAINT customer_pkey PRIMARY KEY (id)
            )

            TABLESPACE pg_default;

            ALTER TABLE simple.customer
                OWNER to postgres;"


PGPASSWORD=password psql \
    -h 127.0.0.1 -d example -U postgres \
    -c "CREATE TABLE simple."order"
            (
                id bigint NOT NULL,
                order_description text COLLATE pg_catalog."default",
                customer_id bigint,
                CONSTRAINT id PRIMARY KEY (id),
                CONSTRAINT customer_id FOREIGN KEY (customer_id)
                    REFERENCES simple.customer (id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
            )

            TABLESPACE pg_default;

            ALTER TABLE simple."order"
                OWNER to postgres;"


PGPASSWORD=password psql \
    -h 127.0.0.1 -d example -U postgres \
    -c "CREATE TABLE simple."item"
            (
                id bigint PRIMARY KEY,
                item_description text COLLATE pg_catalog."default",
                order_id bigint,
                CONSTRAINT order_id FOREIGN KEY (order_id)
                    REFERENCES simple.order (id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
            )

            TABLESPACE pg_default;

            ALTER TABLE simple."item"
                OWNER to postgres;"
                

PGPASSWORD=password psql \
    -h 127.0.0.1 -d example -U postgres \
    -c "INSERT INTO simple.customer(
	        id, description)
	        VALUES (generate_series(1,1000), md5(random()::text));"


#sth wrong with random in shell see markdown for correct sql statement in psql
PGPASSWORD=password psql \
    -h 127.0.0.1 -d example -U postgres \
    -c "INSERT INTO simple.order(
	        id, order_description, customer_id)
	        VALUES (generate_series(1,10000), md5(random()::text), (random() * 999 + 1)::int);"


PGPASSWORD=password psql \
    -h 127.0.0.1 -d example -U postgres \
    -c "INSERT INTO simple.item(
	        id, item_description, order_id)
	        VALUES (generate_series(1,100000), md5(random()::text), (random() * 9999 + 1)::int);"