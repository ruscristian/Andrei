CREATE TABLE accommodation
(
    id serial PRIMARY KEY NOT NULL,
    type character varying(32) NOT NULL,
    bed_type character varying(32) NOT NULL,
    max_guests integer NOT NULL,
    description character varying(512),

);

CREATE TABLE room_fare
(
    id serial PRIMARY KEY NOT NULL,
    value double precision NOT NULL,
    season character varying(32),

);

CREATE TABLE accommodation_fare_relation
(
    id serial PRIMARY KEY NOT NULL,
    id_accommodation integer NOT NULL,
--    FOREIGN KEY (id_accommodation)
--    REFERENCES accommodation (id)
    id_room_fare integer NOT NULL
--    FOREIGN KEY (id_room_fare)
--    REFERENCES room_fare (id)

);
