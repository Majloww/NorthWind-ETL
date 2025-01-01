# **ETL proces datasetu NorthWind**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z **NorthWind** datasetu. Projekt sa zameriava na preskúmanie správania používateľov a ich čitateľských preferencií na základe hodnotení kníh a demografických údajov používateľov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.

---
## **1. Úvod a popis zdrojových dát**
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa kníh, používateľov a ich hodnotení. Táto analýza umožňuje identifikovať trendy v čitateľských preferenciách, najpopulárnejšie knihy a správanie používateľov.

Zdrojové dáta pochádzajú z Kaggle datasetu dostupného [tu](https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/northwind-pubs). Dataset obsahuje päť hlavných tabuliek:
- `books`
- `ratings`
- `users`
- `occupations`
- `education_levels`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/Majloww/NorthWind-ETL/blob/main/Northwind_ERD.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma NorthWind</em>
</p>

---
## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_ratings`**, ktorá je prepojená s nasledujúcimi dimenziami:
- **`dim_books`**: Obsahuje podrobné informácie o knihách (názov, autor, rok vydania, vydavateľ).
- **`dim_users`**: Obsahuje demografické údaje o používateľoch, ako sú vekové kategórie, pohlavie, povolanie a vzdelanie.
- **`dim_date`**: Zahrňuje informácie o dátumoch hodnotení (deň, mesiac, rok, štvrťrok).
- **`dim_time`**: Obsahuje podrobné časové údaje (hodina, AM/PM).

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="https://github.com/Majloww/NorthWind-ETL/blob/main/Star_Schema_Updated.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre NorthWind</em>
</p>

---
## **3. ETL proces v Snowflake**
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `my_stage`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

#### Príklad kódu:
```sql
CREATE OR REPLACE STAGE my_stage;
```
Do stage boli následne nahraté súbory obsahujúce údaje o knihách, používateľoch, hodnoteniach, zamestnaniach a úrovniach vzdelania. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz:

```sql
COPY INTO occupations_staging
FROM @my_stage/occupations.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
```

V prípade nekonzistentných záznamov bol použitý parameter `ON_ERROR = 'CONTINUE'`, ktorý zabezpečil pokračovanie procesu bez prerušenia pri chybách.

---
### **3.1 Transfor (Transformácia dát)**

V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

Dimenzie boli navrhnuté na poskytovanie kontextu pre faktovú tabuľku. `Dim_users` obsahuje údaje o používateľoch vrátane vekových kategórií, pohlavia, zamestnania a vzdelania. Transformácia zahŕňala rozdelenie veku používateľov do kategórií (napr. „18-24“) a pridanie popisov zamestnaní a vzdelania. Táto dimenzia je typu SCD 2, čo umožňuje sledovať historické zmeny v zamestnaní a vzdelaní používateľov.
```sql
CREATE TABLE dim_users AS
SELECT DISTINCT
    u.userId AS dim_userId,
    CASE 
        WHEN u.age < 18 THEN 'Under 18'
        WHEN u.age BETWEEN 18 AND 24 THEN '18-24'
        WHEN u.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN u.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN u.age BETWEEN 45 AND 54 THEN '45-54'
        WHEN u.age >= 55 THEN '55+'
        ELSE 'Unknown'
    END AS age_group,
    u.gender,
    o.name AS occupation,
    e.name AS education_level
FROM users_staging u
JOIN occupations_staging o ON u.occupationId = o.occupationId
JOIN education_levels_staging e ON u.educationId = e.educationId;
```
Dimenzia `dim_date` je navrhnutá tak, aby uchovávala informácie o dátumoch hodnotení kníh. Obsahuje odvodené údaje, ako sú deň, mesiac, rok, deň v týždni (v textovom aj číselnom formáte) a štvrťrok. Táto dimenzia je štruktúrovaná tak, aby umožňovala podrobné časové analýzy, ako sú trendy hodnotení podľa dní, mesiacov alebo rokov. Z hľadiska SCD je táto dimenzia klasifikovaná ako SCD Typ 0. To znamená, že existujúce záznamy v tejto dimenzii sú nemenné a uchovávajú statické informácie.

V prípade, že by bolo potrebné sledovať zmeny súvisiace s odvodenými atribútmi (napr. pracovné dni vs. sviatky), bolo by možné prehodnotiť klasifikáciu na SCD Typ 1 (aktualizácia hodnôt) alebo SCD Typ 2 (uchovávanie histórie zmien). V aktuálnom modeli však táto potreba neexistuje, preto je `dim_date` navrhnutá ako SCD Typ 0 s rozširovaním o nové záznamy podľa potreby.

```sql
CREATE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(timestamp AS DATE)) AS dim_dateID,
    CAST(timestamp AS DATE) AS date,
    DATE_PART(day, timestamp) AS day,
    DATE_PART(dow, timestamp) + 1 AS dayOfWeek,
    CASE DATE_PART(dow, timestamp) + 1
        WHEN 1 THEN 'Pondelok'
        WHEN 2 THEN 'Utorok'
        WHEN 3 THEN 'Streda'
        WHEN 4 THEN 'Štvrtok'
        WHEN 5 THEN 'Piatok'
        WHEN 6 THEN 'Sobota'
        WHEN 7 THEN 'Nedeľa'
    END AS dayOfWeekAsString,
    DATE_PART(month, timestamp) AS month,
    DATE_PART(year, timestamp) AS year,
    DATE_PART(quarter, timestamp) AS quarter
FROM ratings_staging;
```
Podobne `dim_books` obsahuje údaje o knihách, ako sú názov, autor, rok vydania a vydavateľ. Táto dimenzia je typu SCD Typ 0, pretože údaje o knihách sú považované za nemenné, napríklad názov knihy alebo meno autora sa nemenia. 

Faktová tabuľka `fact_ratings` obsahuje záznamy o hodnoteniach a prepojenia na všetky dimenzie. Obsahuje kľúčové metriky, ako je hodnota hodnotenia a časový údaj.
```sql
CREATE TABLE fact_ratings AS
SELECT 
    r.ratingId AS fact_ratingID,
    r.timestamp AS timestamp,
    r.rating,
    d.dim_dateID AS dateID,
    t.dim_timeID AS timeID,
    b.dim_bookId AS bookID,
    u.dim_userId AS userID
FROM ratings_staging r
JOIN dim_date d ON CAST(r.timestamp AS DATE) = d.date
JOIN dim_time t ON r.timestamp = t.timestamp
JOIN dim_books b ON r.ISBN = b.dim_bookId
JOIN dim_users u ON r.userId = u.dim_userId;
```

---
### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:
```sql
DROP TABLE IF EXISTS books_staging;
DROP TABLE IF EXISTS education_levels_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS users_staging;
```
ETL proces v Snowflake umožnil spracovanie pôvodných dát z `.csv` formátu do viacdimenzionálneho modelu typu hviezda. Tento proces zahŕňal čistenie, obohacovanie a reorganizáciu údajov. Výsledný model umožňuje analýzu čitateľských preferencií a správania používateľov, pričom poskytuje základ pre vizualizácie a reporty.

---
## **4 Vizualizácia dát**

Dashboard obsahuje `6 vizualizácií`, ktoré poskytujú základný prehľad o kľúčových metrikách a trendoch týkajúcich sa kníh, používateľov a hodnotení. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť správanie používateľov a ich preferencie.

<p align="center">
  <img src="https://github.com/JKabathova/NorthWind-ETL/blob/master/NorthWind_dashboard.png" alt="ERD Schema">
  <br>
  <em>Obrázok 3 Dashboard NorthWind datasetu</em>
</p>

---
### **Graf 1: Najviac hodnotené knihy (Top 10 kníh)**
Táto vizualizácia zobrazuje 10 kníh s najväčším počtom hodnotení. Umožňuje identifikovať najpopulárnejšie tituly medzi používateľmi. Zistíme napríklad, že kniha `Wild Animus` má výrazne viac hodnotení v porovnaní s ostatnými knihami. Tieto informácie môžu byť užitočné na odporúčanie kníh alebo marketingové kampane.

```sql
SELECT 
    b.title AS book_title,
    COUNT(f.fact_ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_BOOKS b ON f.bookID = b.dim_bookId
GROUP BY b.title
ORDER BY total_ratings DESC
LIMIT 10;
```
---
### **Graf 2: Rozdelenie hodnotení podľa pohlavia používateľov**
Graf znázorňuje rozdiely v počte hodnotení medzi mužmi a ženami. Z údajov je zrejmé, že ženy hodnotili knihy o niečo častejšie ako muži, no rozdiely sú minimálne a aktivita medzi pohlaviami je viac-menej vyrovnaná. Táto vizualizácia ukazuje, že obsah alebo kampane môžu byť efektívne zamerané na obe pohlavia bez potreby výrazného rozlišovania.

```sql
SELECT 
    u.gender,
    COUNT(f.fact_ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.dim_userId
GROUP BY u.gender;
```
---
### **Graf 3: Trendy hodnotení kníh podľa rokov vydania (2000–2024)**
Graf ukazuje, ako sa priemerné hodnotenie kníh mení podľa roku ich vydania v období 2000–2024. Z vizualizácie je vidieť, že medzi rokmi 2000 a 2005 si knihy udržiavali stabilné priemerné hodnotenie. Po tomto období však nastal výrazný pokles priemerného hodnotenia. Od tohto bodu opäť postupne stúpajú a  po roku 2020, je tendencia, že knihy získavajú vyššie priemerné hodnotenia. Tento trend môže naznačovať zmenu kvality kníh, vývoj čitateľských preferencií alebo rozdiely v hodnotiacich kritériách používateľov.

```sql
SELECT 
    b.release_year AS year,
    AVG(f.rating) AS avg_rating
FROM FACT_RATINGS f
JOIN DIM_BOOKS b ON f.bookID = b.dim_bookId
WHERE b.release_year BETWEEN 2000 AND 2024
GROUP BY b.release_year
ORDER BY b.release_year;
```
---
### **Graf 4: Celková aktivita počas dní v týždni**
Tabuľka znázorňuje, ako sú hodnotenia rozdelené podľa jednotlivých dní v týždni. Z údajov vyplýva, že najväčšia aktivita je zaznamenaná cez víkendy (sobota a nedeľa) a počas dní na prelome pracovného týždňa a víkendu (piatok a pondelok). Tento trend naznačuje, že používatelia majú viac času na čítanie a hodnotenie kníh počas voľných dní.

```sql
SELECT 
    d.dayOfWeekAsString AS day,
    COUNT(f.fact_ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_DATE d ON f.dateID = d.dim_dateID
GROUP BY d.dayOfWeekAsString
ORDER BY total_ratings DESC;
```
---
### **Graf 5: Počet hodnotení podľa povolaní**
Tento graf  poskytuje informácie o počte hodnotení podľa povolaní používateľov. Umožňuje analyzovať, ktoré profesijné skupiny sú najviac aktívne pri hodnotení kníh a ako môžu byť tieto skupiny zacielené pri vytváraní personalizovaných odporúčaní. Z údajov je zrejmé, že najaktívnejšími profesijnými skupinami sú `Marketing Specialists` a `Librarians`, s viac ako 1 miliónom hodnotení. 

```sql
SELECT 
    u.occupation AS occupation,
    COUNT(f.fact_ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_USERS u ON f.userID = u.dim_userId
GROUP BY u.occupation
ORDER BY total_ratings DESC
LIMIT 10;
```
---
### **Graf 6: Aktivita používateľov počas dňa podľa vekových kategórií**
Tento stĺpcový graf ukazuje, ako sa aktivita používateľov mení počas dňa (dopoludnia vs. popoludnia) a ako sa líši medzi rôznymi vekovými skupinami. Z grafu vyplýva, že používatelia vo vekovej kategórii `55+` sú aktívni rovnomerne počas celého dňa, zatiaľ čo ostatné vekové skupiny vykazujú výrazne nižšiu aktivitu a majú obmedzený čas na hodnotenie, čo môže súvisieť s pracovnými povinnosťami. Tieto informácie môžu pomôcť lepšie zacieliť obsah a plánovať aktivity pre rôzne vekové kategórie.
```sql
SELECT 
    t.ampm AS time_period,
    u.age_group AS age_group,
    COUNT(f.fact_ratingID) AS total_ratings
FROM FACT_RATINGS f
JOIN DIM_TIME t ON f.timeID = t.dim_timeID
JOIN DIM_USERS u ON f.userID = u.dim_userId
GROUP BY t.ampm, u.age_group
ORDER BY time_period, total_ratings DESC;

```

Dashboard poskytuje komplexný pohľad na dáta, pričom zodpovedá dôležité otázky týkajúce sa čitateľských preferencií a správania používateľov. Vizualizácie umožňujú jednoduchú interpretáciu dát a môžu byť využité na optimalizáciu odporúčacích systémov, marketingových stratégií a knižničných služieb.

---

**Autor:** Miloš Chmelko
