
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


1. **Etapa: Vytvorenie databázy a staging tabuliek**
- Vytvorenie databázy a schémy: Na začiatku ste vytvorili databázu MovieDB a schému MovieDB.staging,
 ktorá slúži ako dočasná schéma pre spracovanie dát.
- Vytvorenie staging tabuliek: Následne ste vytvorili niekoľko staging tabuliek, ktoré slúžia na uchovávanie neformátovaných dát pred ich transformáciou.
 Tieto tabuľky obsahujú údaje o filmoch, menách hercov, žánroch, hodnoteniach a ďalších premenných (napr. movie_staging, names_staging, genre_staging, atď.).
2. **Etapa: Načítanie dát a transformácia**
- Načítanie dát do staging tabuliek: Dátové súbory (napr. CSV súbory) sú načítané do vytvorených staging tabuliek pomocou príkazov COPY INTO,
 kde sú definované formáty CSV a preskočenie hlavičiek.
- Vytvorenie dočasných tabuliek: Na spracovanie konkrétnych dát, ako sú herci a režiséri,
 ste vytvorili dočasné tabuľky (role_and_director_mapping_staging), ktoré spájajú hercov s filmami.
- Vytvorenie dimenzií: V tejto fáze ste vytvorili dimenzionálne tabuľky 
 ako dim_movie, dim_names, dim_genre, dim_category, atď. Tieto tabuľky uchovávajú podrobné informácie o filmoch, hercoch, žánroch, kategóriách a ďalších entitách.
3. **Etapa: Načítanie dát do finálnych dimenzií a faktovej tabuľky**
- Načítanie dát do dimenzií: Dátové transformácie na získanie správnych hodnôt do dimenzií prebiehajú pomocou príkazov INSERT INTO.
 Týmto spôsobom sa do dimenzií načítajú unikalne údaje ako názvy filmov, mená hercov, žánre a pod.
- Vytvorenie faktovej tabuľky (fact_ratings): Vytvorili ste faktovú tabuľku fact_ratings, ktorá uchováva údaje o hodnotení filmov,
 vrátane priemerného hodnotenia, počtu hlasov, mediánu a priradených ID z dimenzií ako dim_movie, dim_genre, dim_names a dim_category.
- Vkladanie dát do faktovej tabuľky: Na záver ste do faktovej tabuľky vložili údaje z staging tabuliek a dimenzií,
 pričom ste zabezpečili správne prepojenia medzi faktovou tabuľkou a dimenziami.
- Čistenie a odstránenie staging tabuliek: Po načítaní a spracovaní dát ste odstránili staging tabuľky,
 aby ste uvoľnili miesto v databáze.


### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `STAGE`.
Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát.


