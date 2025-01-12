use MOVIEDB.STAGING;

//1. Priemerná dĺžka filmu podľa krajín
SELECT 
    dc.country_name AS country,
    ROUND(AVG(dm.duration), 2) AS average_movie_duration
FROM 
    dim_movie dm
JOIN 
    fact_ratings fr ON fr.dim_movie_id = dm.id
JOIN 
    LATERAL FLATTEN(INPUT => dm.country_ids) AS flattened_countries
JOIN 
    dim_countries dc ON dc.id_country = flattened_countries.VALUE
WHERE 
    fr.avg_rating > 7
    AND dm.duration IS NOT NULL
GROUP BY 
    dc.country_name
ORDER BY 
    average_movie_duration DESC
    limit 10 ;

    
//2. Celkový zárobok USA podľa rokov
SELECT
    EXTRACT(YEAR FROM dm.date_published) AS year,
    SUM(CAST(REPLACE(REPLACE(NULLIF(dm.worldwide_gross_income, 'NULL'), '$', ''), ',', '') AS DECIMAL(15, 2))) AS total_income
FROM
    dim_movie dm
JOIN
    LATERAL FLATTEN(INPUT => dm.country_ids) AS flattened_countries
JOIN
    dim_countries dc ON dc.id_country = flattened_countries.VALUE
WHERE
    dc.country_name = 'USA'
    AND EXTRACT(YEAR FROM dm.date_published) IN (2017, 2018, 2019)
    AND dm.worldwide_gross_income IS NOT NULL
GROUP BY
    EXTRACT(YEAR FROM dm.date_published)
ORDER BY
    year;

    
//3.  Priemerné hodnotenie podľa hercov
SELECT
    n.name AS actor_name,
    ROUND(AVG(f.avg_rating), 2) AS avg_rating_per_actor,
    COUNT(DISTINCT f.dim_movie_id) AS movie_count
FROM
    fact_ratings f
JOIN dim_names n ON f.dim_names_id = n.id_dim_names
JOIN dim_movie m ON f.dim_movie_id = m.id
WHERE
    f.avg_rating IS NOT NULL 
GROUP BY
    n.name
HAVING
    COUNT(DISTINCT f.dim_movie_id) > 5 
ORDER BY
    avg_rating_per_actor DESC
LIMIT
    10;

    
//4. Počet filmov podľa krajín
SELECT
    dc.country_name AS country,
    COUNT(DISTINCT dm.id) AS num_movies
FROM
    dim_movie dm
    JOIN LATERAL FLATTEN(INPUT => dm.country_ids) AS flattened_countries
    JOIN dim_countries dc ON dc.id_country = flattened_countries.VALUE
GROUP BY
    dc.country_name
ORDER BY
    num_movies DESC
limit 10;

//5 .Top 5 zánrov filmov
SELECT
    dg.genre_name AS genre,
    COUNT(dm.id) AS movie_count
FROM
    dim_movie dm
JOIN
    fact_ratings fr ON dm.id = fr.dim_movie_id
JOIN
    dim_genre dg ON fr.dim_genre_id = dg.id_genre
WHERE
    EXTRACT(YEAR FROM dm.date_published) BETWEEN 2017 AND 2019
GROUP BY
    dg.genre_name
ORDER BY
    movie_count DESC
LIMIT 5;

    
//6. Počet filmov podľa hodnotenia
SELECT
    CASE
        WHEN avg_rating >= 1.0
        AND avg_rating <= 3.9 THEN '1.0-3.9 (Low)'
        WHEN avg_rating >= 4.0
        AND avg_rating <= 6.9 THEN '4.0-6.9 (Medium)'
        WHEN avg_rating >= 7.0
        AND avg_rating <= 10.0 THEN '7.0-10.0 (High)'
    END AS rating_group,
    COUNT(DISTINCT dim_movie_id) AS num_movies
FROM
    fact_ratings
GROUP BY
    CASE
        WHEN avg_rating >= 1.0
        AND avg_rating <= 3.9 THEN '1.0-3.9 (Low)'
        WHEN avg_rating >= 4.0
        AND avg_rating <= 6.9 THEN '4.0-6.9 (Medium)'
        WHEN avg_rating >= 7.0
        AND avg_rating <= 10.0 THEN '7.0-10.0 (High)'
    END
ORDER BY
    rating_group;

       
