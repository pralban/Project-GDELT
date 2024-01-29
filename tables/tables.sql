CREATE TABLE gdelt.request_1_2 (
    global_event_id INT,
    event_year INT,
    event_month INT,
    event_day INT,
    source_language TEXT,
    event_country TEXT,
    event_country_full_name TEXT,
    num_mentions INT,
    PRIMARY KEY (global_event_id)
);

CREATE TABLE gdelt.request_3_4 (  
    global_event_id TEXT,
    internet_source TEXT,
    event_year INT,
    event_month INT,
    event_day INT,
    event_themes TEXT,
    person_list TEXT,
    event_tone DOUBLE,
    first_country TEXT,
    second_country TEXT,
    PRIMARY KEY (global_event_id)
);
