-- Vytvorenie databázy a schémy


CREATE DATABASE MovieDB;
CREATE SCHEMA MovieDB.staging;
USE SCHEMA MovieDB.staging;


-- Vytvorenie staging tabuliek
//


CREATE TABLE movie_staging (
    id VARCHAR(10) PRIMARY KEY,
    title VARCHAR(200),
    year INT,
    date_published DATE,
    duration INT,
    country VARCHAR(250),
    worldwide_gross_income VARCHAR(30),
    languages VARCHAR(200),
    production_company VARCHAR(200)
);


CREATE TABLE names_staging (
    id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100),
    height INT,
    date_of_birth DATE,
    known_for_movies VARCHAR(100)
);


CREATE TABLE genre_staging (
    movie_id VARCHAR(10),
    genre VARCHAR(20),
    FOREIGN KEY (movie_id) REFERENCES movie_staging(id)
);


CREATE TABLE ratings_staging (
    movie_id VARCHAR(10),
    avg_rating DECIMAL(4, 2),
    total_votes INT,
    median_rating INT,
    FOREIGN KEY (movie_id) REFERENCES movie_staging(id)
);


CREATE TABLE director_mapping_staging (
    movie_id VARCHAR(10),
    names_id VARCHAR(10),
    FOREIGN KEY (movie_id) REFERENCES movie_staging(id),
    FOREIGN KEY (names_id) REFERENCES names_staging(id)
);


CREATE TABLE role_mapping_staging (
    movie_id VARCHAR(10),
    names_id VARCHAR(10),
    category VARCHAR(10),
    FOREIGN KEY (movie_id) REFERENCES movie_staging(id),
    FOREIGN KEY (names_id) REFERENCES names_staging(id)
);


//
CREATE OR REPLACE STAGE movie_data_stage;

//
-- Načítanie dát do staging tabuliek
COPY INTO movie_staging
FROM @movie_data_stage/movie.csv 
FILE_FORMAT = ( TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 );


COPY INTO genre_staging
FROM @movie_data_stage/genre.csv 
FILE_FORMAT = ( TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 );


COPY INTO ratings_staging
FROM @movie_data_stage/ratings.csv 
FILE_FORMAT = ( TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 );


COPY INTO director_mapping_staging
FROM @movie_data_stage/director_mapping.csv 
FILE_FORMAT = ( TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 );


COPY INTO role_mapping_staging
FROM @movie_data_stage/role_mapping.csv 
FILE_FORMAT = ( TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 );


COPY INTO names_staging
FROM @movie_data_stage/names.csv 
FILE_FORMAT = ( TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('NULL'));


//
    -- Vytvorenie tabulky
CREATE TABLE role_and_director_mapping_staging AS
SELECT
    movie_id,
    names_id,
    'Director' AS category
FROM
    MovieDB.staging.director_mapping_staging
UNION
SELECT
    movie_id,
    names_id,
    category
FROM
    MovieDB.staging.role_mapping_staging;

    
CREATE TABLE dim_movie (
        id VARCHAR(10) PRIMARY KEY,
        title VARCHAR(200),
        year INT,
        date_published DATE,
        duration INT,
        country VARCHAR(250),
        worldwide_gross_income VARCHAR(30),
        languages VARCHAR(200),
        production_company VARCHAR(200)
);

    
CREATE TABLE dim_names (
        id_dim_names VARCHAR(10) PRIMARY KEY,
        name VARCHAR(100),
        height INT,
        date_of_birth DATE,
        known_for_movies VARCHAR(100)
);

    
CREATE SEQUENCE dim_genre_seq START WITH 1 INCREMENT BY 1;
CREATE TABLE dim_genre (
        id_genre INT DEFAULT dim_genre_seq.NEXTVAL PRIMARY KEY,
        genre_name VARCHAR(100) NOT NULL
);

    
CREATE SEQUENCE category_id_seq START WITH 1 INCREMENT BY 1;
CREATE TABLE dim_category (
        id_dim_category INT DEFAULT category_id_seq.NEXTVAL PRIMARY KEY,
        category VARCHAR(100) NOT NULL
);

    
CREATE SEQUENCE ratings_id_seq START WITH 1 INCREMENT BY 1;
CREATE TABLE fact_ratings (
        fact_ratings_id INT DEFAULT ratings_id_seq.NEXTVAL PRIMARY KEY,
        avg_rating DECIMAL(4, 2),
        total_votes INT,
        median_rating INT,
        dim_movie_id VARCHAR(10),
        dim_genre_id INT,
        dim_names_id VARCHAR(10),
        dim_category_id INT,
        FOREIGN KEY (dim_movie_id) REFERENCES dim_movie(id),
        FOREIGN KEY (dim_genre_id) REFERENCES dim_genre(id_genre),
        FOREIGN KEY (dim_names_id) REFERENCES dim_names(id_dim_names),
        FOREIGN KEY (dim_category_id) REFERENCES dim_category(id_dim_category)
);
//
INSERT INTO dim_movie (
    id,
    title,
    year,
    date_published,
    duration,
    country,
    worldwide_gross_income,
    languages,
    production_company
)
SELECT
    id,
    title,
    year,
    date_published,
    duration,
    country,
    worldwide_gross_income,
    languages,
    production_company
FROM movie_staging;

    
INSERT INTO dim_names (
    id_dim_names,
    name,
    height,
    date_of_birth,
    known_for_movies
)
SELECT
    id,
    name,
    height,
    date_of_birth,
    known_for_movies
FROM names_staging;

    
INSERT INTO dim_genre (genre_name)
SELECT
    DISTINCT genre
FROM genre_staging;

    
INSERT INTO dim_category (category)
SELECT
    DISTINCT category
FROM role_and_director_mapping_staging;

    
//
    -- Vkladanie dát do fact_ratings
INSERT INTO fact_ratings (
    avg_rating,
    total_votes,
    median_rating,
    dim_movie_id,
    dim_genre_id,
    dim_names_id,
    dim_category_id
)
SELECT
    rs.avg_rating,
    rs.total_votes,
    rs.median_rating,
    m.id AS dim_movie_id,
    dg.id_genre AS dim_genre_id,
    n.id_dim_names AS dim_names_id,
    dc.id_dim_category AS dim_category_id
FROM
    ratings_staging rs
    JOIN movie_staging m ON rs.movie_id = m.id
    LEFT JOIN genre_staging gs ON rs.movie_id = gs.movie_id
    LEFT JOIN dim_genre dg ON gs.genre = dg.genre_name
    LEFT JOIN role_and_director_mapping_staging rdms ON rs.movie_id = rdms.movie_id
    LEFT JOIN dim_names n ON rdms.names_id = n.id_dim_names
    LEFT JOIN dim_category dc ON rdms.category = dc.category
WHERE
    rs.avg_rating IS NOT NULL
    AND rs.total_votes IS NOT NULL
    AND rs.median_rating IS NOT NULL;

    
//
    -- Odstránenie
    
DROP TABLE IF EXISTS movie_staging;
DROP TABLE IF EXISTS names_staging;
DROP TABLE IF EXISTS genre_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS director_mapping_staging;
DROP TABLE IF EXISTS role_mapping_staging;


//
    
-- Vytvorenie sekvencie a tabuľky pre jazyky


CREATE SEQUENCE dim_language_seq START WITH 1 INCREMENT BY 1;

CREATE TABLE dim_languages (
        id_language INT DEFAULT dim_language_seq.NEXTVAL PRIMARY KEY,
        language_name VARCHAR(100) UNIQUE
);

    
INSERT INTO dim_languages (language_name)
SELECT
    DISTINCT TRIM(VALUE) AS language_name
FROM
    dim_movie, LATERAL FLATTEN(INPUT => SPLIT(languages, ','))
WHERE
    TRIM(VALUE) IS NOT NULL;

    
-- Aktualizácia dim_movie o ID jazykov


ALTER TABLE dim_movie add COLUMN languages_ids ARRAY;


CREATE TEMPORARY TABLE temp_language_map AS
SELECT
    dm.id AS movie_id,
    ARRAY_AGG(dl.id_language) AS language_ids
FROM
    dim_movie dm
    JOIN dim_languages dl ON POSITION(TRIM(dl.language_name) IN dm.languages) > 0
GROUP BY dm.id;
MERGE INTO dim_movie dm USING temp_language_map tm ON dm.id = tm.movie_id
WHEN MATCHED THEN
UPDATE SET dm.language_ids = tm.language_ids;


DROP TABLE temp_language_map;
ALTER TABLE dim_movie drop COLUMN languages;


-- Vytvorenie sekvencie a tabuľky pre krajiny


CREATE SEQUENCE dim_country_seq START WITH 1 INCREMENT BY 1;

CREATE TABLE dim_countries (
        id_country INT DEFAULT dim_country_seq.NEXTVAL PRIMARY KEY,
        country_name VARCHAR(100) UNIQUE
);

    
-- Vkladanie krajín do dim_countries


INSERT INTO dim_countries (country_name)
SELECT
    DISTINCT TRIM(VALUE) AS country_name
FROM
    dim_movie,
    LATERAL FLATTEN(INPUT => SPLIT(country, ','))
WHERE
    TRIM(VALUE) IS NOT NULL;

    
-- Aktualizácia dim_movie o ID krajín


ALTER TABLE dim_movie ADD COLUMN country_ids ARRAY;


CREATE TEMPORARY TABLE temp_country_map AS
SELECT
    dm.id AS movie_id,
    ARRAY_AGG(dc.id_country) AS country_ids
FROM
    dim_movie dm
    JOIN dim_countries dc ON POSITION(TRIM(dc.country_name) IN dm.country) > 0
GROUP BY
    dm.id;
MERGE INTO dim_movie dm
USING temp_country_map tm
ON dm.id = tm.movie_id
WHEN MATCHED THEN
UPDATE SET dm.country_ids = tm.country_ids;



DROP TABLE temp_country_map;
ALTER TABLE dim_movie drop COLUMN country;




