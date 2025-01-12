
# **ETL proces datasetu IMDb**
    Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z IMDb.
Projekt sa zameriava na preskúmanie preferencií divákov, ich správania a charakteristík filmov na základe hodnotení, žánrov a filmových tímov. Výsledný model dát umožňuje vykonávať viacrozmernú analýzu a vizualizovať kľúčové metriky.
_______________________

## **1. Úvod a popis zdrojových dát**
    Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa filmov, divákov a ich hodnotení.
Táto analýza umožňuje identifikovať trendy v preferenciách divákov, najpopulárnejšie filmy a správanie používateľov.

Zdrojové dáta pochádzajú z EDU datasetu dostupného [tu](https://edu.ukf.sk/mod/folder/view.php?id=252868). Dataset obsahuje
## Hlavné tabuľky:
- `movie`
- `ratings`
- `genre`
- `names`

## Podriadené tabuľky:
- `director_mapping`
- `role_mapping`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.
_______________________


### **1.1 Dátová architektúra**

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="IMDB_ERD.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma IMDb</em>
</p>


_______________________


## **2. Dimenzionálny model**

Navrhnutý bol hviezdicový model **(Snowflake schema)** pre efektívnu analýzu, kde centrálny bod tvorí faktová tabuľka **'fact_ratings'**, ktorá je prepojená s nasledujúcimi dimenziami:

- **'dim_movie'**: Obsahuje podrobné informácie o filmoch (názov, rok vydania, dátum publikácie, dĺžka trvania, krajina, celosvetový príjem, jazyky, produkčná spoločnosť).

- **'dim_names'**: Uchováva informácie o menách (meno, výška, dátum narodenia, známe filmy).

- **'dim_genre'**: Obsahuje informácie o žánroch filmov (názov žánru).

- **'dim_category'**: Uchováva kategórie pre hercov a režisérov (režisér, herec, atď.).

- **'dim_languages'**: Obsahuje jazyky, v ktorých boli filmy vydané.

- **'dim_countries'**: Uchováva krajiny, v ktorých boli filmy vydané.

<p align="center">
  <img src="..." alt="Snowflake Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre IMDb</em>
</p>

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami

## **3. ETL proces v Snowflake**
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). 
Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.


### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu vo formáte `.csv` boli najprv nahrané do Snowflake cez interné stage úložisko s názvom movie_data_stage. Stage slúži ako dočasné úložisko na rýchle nahrávanie a spracovanie dát. Vytvorenie stage bolo zabezpečené príkazom:

```sql 
    CREATE OR REPLACE STAGE movie_data_stage;
 ```

Po vytvorení stage, boli doň nahraté súbory obsahujúce údaje o filmoch, hercoch, žánroch, hodnoteniach, režiséroch a kategóriách. Na nahrávanie dát do staging tabuliek sa použil príkaz COPY INTO, ktorý importoval dáta z jednotlivých súborov (napríklad movie.csv, ratings.csv, names.csv a iné).

Príklad kódu na načítanie dát:

```sql 
    COPY INTO movie_staging
    FROM @movie_data_stage/movie.csv
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
 ```
V prípade, že počas nahrávania došlo k nekompletným alebo nekonzistentným záznamom, bol použitý parameter ON_ERROR = 'CONTINUE', ktorý umožnil pokračovanie procesu bez prerušenia, pričom chyby sa ignorovali.

Tieto dáta, po nahratí do staging tabuliek, poskytujú základ pre ďalšie spracovanie v ďalších krokoch ETL procesu, kde budú transformované a neskôr uložené v dimenziách a faktových tabuľkách pre analytické účely.

### **3.2 Transfor (Transformácia dát)**

V rámci transformácie bolo zmenených niekoľko tabuliek: `movie_staging`, `genre_staging`, `director_mapping_staging` a `role_mapping_staging`.

3.2.1. 
`movie_staging` : 
movie_staging obsahovala polia s viacerými jazykmi a krajinami, ktoré boli uložené ako zoznamy. Preto som sa rozhodol tieto údaje normalizovať a presunúť ich do samostatných dimenzionálnych tabuliek dim_languages a dim_countries, pričom každému záznamu bol priradený jedinečný identifikátor (ID).

Príklad kódu:

```sql 
CREATE SEQUENCE dim_language_seq START WITH 1 INCREMENT BY 1;

CREATE TABLE dim_languages (
  id_language INT DEFAULT dim_language_seq.NEXTVAL PRIMARY KEY,
  language_name VARCHAR(100) UNIQUE
);
```
Takéto vytvorenie tabuliek umožňuje presnejšiu analýzu dát a vytváranie prehľadnejších grafov, keďže údaje sú normalizované a rozdelené do samostatných dimenzií, čo zjednodušuje ich spracovanie a interpretáciu.

Aby údaje vyzerali ako zoznam, použil som funkciu LATERAL FLATTEN v kombinácii s rozdelením reťazca pomocou funkcie SPLIT. Týmto spôsobom boli hodnoty oddelené a spracované ako jednotlivé záznamy, čo umožnilo ich jednoduché vloženie do dimenzionálnych tabuliek.

Príklad kódu:
```sql 
INSERT INTO dim_languages (language_name)
SELECT
  DISTINCT TRIM(VALUE) AS language_name
FROM
  dim_movie, LATERAL FLATTEN(INPUT => SPLIT(languages, ','))
WHERE
  TRIM(VALUE) IS NOT NULL;
```
Vytvorenie dočasných máp
Použitím dočasných tabuliek (temp_language_map a temp_country_map) boli prepojené ID jazykov a krajín s filmami:

temp_language_map: Obsahuje mapovanie medzi filmami a ich jazykmi.
temp_country_map: Obsahuje mapovanie medzi filmami a krajinami.

Dočasná tabuľka slúži na priradenie jedinečných ID jazykov z tabuľky dim_languages k jednotlivým filmom v tabuľke dim_movie. Pôvodné údaje o jazykoch vo filme boli uložené ako textové zoznamy. Táto operácia zabezpečuje, že každý jazyk je reprezentovaný svojím ID, čím sa zjednodušuje manipulácia s dátami. Tento proces je realizovaný príkazom:
```sql 
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
```

Dočasná tabuľka zabezpečuje prevod pôvodného textového zoznamu jazykov na štruktúrovaný formát. Namiesto toho, aby sa jazyk ukladal ako text (napríklad "angličtina, španielčina"), sú jazykové hodnoty mapované na konkrétne ID z dim_languages a ukladané ako pole ID (ARRAY) v tabuľke dim_movie. Tento proces je realizovaný prostredníctvom:

```sql 
MERGE INTO dim_movie dm
USING temp_language_map tm
ON dm.id = tm.movie_id
WHEN MATCHED THEN
UPDATE SET dm.language_ids = tm.language_ids;
```
Celkovo, dočasná tabuľka zohráva kľúčovú rolu pri transformácii dát do čistejšieho, normalizovaného formátu, čo zabezpečuje lepšiu integritu, konzistenciu a efektívnosť pri ďalšom spracovaní a analýze dát.

!Po vykonaní SQL dotazov odstránime všetky dočasné tabuľky, ktoré sú už nepotrebné pre daný časový úsek.!


3.2.2
`genre_staging` : 
Tabuľka genre_staging obsahovala údaje o žánroch filmov, ktoré boli pôvodne uložené ako textové hodnoty. Aby som tieto údaje normalizoval a zjednodušil ich ďalšie spracovanie, rozhodol som sa vytvoriť samostatnú dimenzionálnu tabuľku dim_genre. V tejto tabuľke budú žánre uložené ako jedinečné hodnoty s priradenými ID, čím sa umožní efektívnejšie spracovanie a analýza dát.

```sql 
CREATE SEQUENCE dim_genre_seq START WITH 1 INCREMENT BY 1;

CREATE TABLE dim_genre (
    id_genre INT DEFAULT dim_genre_seq.NEXTVAL PRIMARY KEY,
    genre_name VARCHAR(100) NOT NULL
);
```
Následne som vložil jedinečné žánre zo genre_staging do tabuľky dim_genre, aby som zabezpečil, že každý žáner bude reprezentovaný len raz. Tento krok pomohol odstrániť redundanciu a zjednodušil analýzu:


```sql 
INSERT INTO dim_genre (genre_name)
SELECT DISTINCT genre
FROM genre_staging;
```


3.2.3
`director_mapping_staging` a `role_mapping_staging`:

Takže, rovnako ako pre filmy, aj pre kategórie som vytvoril dočasnú tabuľku, aby som skombinoval všetky potrebné údaje pre novú tabuľku dim_category.

Príklad kódu:
```sql 
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
```
Po tom, ako boli všetky údaje skombinované na jednom mieste, som vytvoril novú tabuľku dim_category, ktorá bude v budúcnosti prepojená s hlavnou tabuľkou fact_ratings.

Príklad kódu:

```sql 
CREATE SEQUENCE category_id_seq START WITH 1 INCREMENT BY 1;
CREATE TABLE dim_category (
    id_dim_category INT DEFAULT category_id_seq.NEXTVAL PRIMARY KEY,
    category VARCHAR(100) NOT NULL
);
```
Príkaz `SELECT DISTINCT category` zabezpečuje, že sa do dim_category vloží len unikátna kategória, čím sa vytvárajú jedinečné záznamy v dimenzii kategórií pre ďalšiu analýzu.

3.2.4
`fact_ratings`:
Fact_ratings tabuľka bola upravená hlavne v súvislosti s pridaním nových cudzích kľúčov a priradením správnych ID z dimenzií (ako dim_movie, dim_genre, dim_names a dim_category). Tento proces zabezpečuje, že všetky faktové údaje v tabuľke fact_ratings sú prepojené so správnymi dimenziami cez cudzí kľúč, čím sa umožňuje efektívne spracovanie a analýzu dát.

Príklad kódu:
```sql 
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

```
### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahrané do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:

```sql 
DROP TABLE IF EXISTS movie_staging;
DROP TABLE IF EXISTS genre_staging;
DROP TABLE IF EXISTS director_mapping_staging;
DROP TABLE IF EXISTS role_mapping_staging;
DROP TABLE IF EXISTS ratings_staging;
```

ETL proces v Snowflake umožnil spracovanie pôvodných dát z rôznych staging tabuliek do viacdimenzionálneho modelu typu hviezda. Tento proces zahŕňal čistenie, obohacovanie a reorganizáciu údajov. Výsledný model umožňuje efektívnu analýzu filmov, hodnotení, žánrov a ďalších faktorov, pričom poskytuje základ pre reporty a vizualizácie.

## **4 Vizualizácia dát**

<p align="center">
  <img src="" alt="ERD Schema">
  <br>
  <em>Obrázok 3 Dashboard IMDb datasetu</em>
</p>

### **Graf 1:**

### **Graf 2:**

### **Graf 3:**

### **Graf 4:**

### **Graf 5:**

### **Graf 6:**
