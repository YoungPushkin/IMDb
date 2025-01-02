Name projekt: IMDb 

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


## **2 Dimenzionálny model**

<p align="center">
  <img src="..." alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre IMDb</em>
</p>



Author: Zadoia Rodion  