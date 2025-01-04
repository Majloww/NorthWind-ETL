# **ETL proces datasetu NorthWind**

Tento projekt predstavuje implementáciu ETL procesu v Snowflake na analýzu dát z Northwind databázy. Cieľom je preskúmať obchodné aktivity spoločnosti prostredníctvom údajov o predajoch, zákazníkoch, produktoch a zamestnancoch. Výsledný dátový model umožňuje podrobnejšiu analýzu dát a poskytuje možnosť vizualizovať rôzne aspekty ako sú predajné trendy, výkonnosť zamestnancov a nákupné preferencie zákazníkov.

---
## **1. Úvod a popis zdrojových dát**
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa produktov, zákazníkov a objednávok. Táto analýza umožňuje identifikovať trendy v predajoch, výkonnosť zamestnancov a preferencie zákazníkov.

Zdrojové dáta pochádzajú z Northwind databázy dostupnej [tu](https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/northwind-pubs). Dataset obsahuje sedem hlavných tabuliek:
- `categories`
- `products`
- `suppliers`
- `customers`
- `employees`
- `orders`
- `shippers`

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

Navrhnutý bol **hviezdicový model (star schema)**, ktorý umožňuje efektívnu analýzu. Centrálny bod tohto modelu tvorí faktová tabuľka **`orders_facts`**, ktorá je prepojená s nasledujúcimi dimenziami:

- **`customers_dim`**: Obsahuje informácie o zákazníkoch (ID zákazníka, meno, kontaktné údaje, mesto, krajina).
- **`employees_dim`**: Zobrazuje údaje o zamestnancoch (ID zamestnanca, meno, priezvisko, dátum narodenia, poznámky).
- **`date_dim`**: Zahrňuje informácie o dátumoch objednávok (dátum, rok, mesiac, deň, týždeň, štvrťrok).
- **`products_dim`**: Obsahuje detaily o produktoch (ID produktu, názov, kategória, jednotka, popis).
- **`shippers_dim`**: Obsahuje údaje o dopravcoch (ID dopravcu, meno dopravcu, telefón).
- **`suppliers_dim`**: Uvádza informácie o dodávateľoch (ID dodávateľa, názov, kontaktné údaje, krajina).

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="https://github.com/Majloww/NorthWind-ETL/blob/main/Star_Schema Final.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Hviezdicová schéma pre NorthWind</em>
</p>

---
## **3. ETL proces v Snowflake**
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu, ktorý je optimalizovaný na analýzu a vizualizáciu.

---
### **3.1 Extrahovanie dát**
V tomto kroku boli najprv dáta zo zdroja vo forme `.csv` nahraté do Snowflake do stage úložiska s názvom `GORILLA_Stage`. Stage slúži ako dočasné úložisko na import alebo export dát.

Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `my_stage`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom: 

#### Príklad kódu:
```sql
CREATE OR REPLACE STAGE GORILLA_Stage;
```
Do stage boli následne nahraté súbory obsahujúce údaje o kategóriách, zákazníkoch, zamestnancoch, objednávkach, produktoch, dodávateľoch a dopravcoch. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`.
```sql
COPY INTO categories_staging
FROM @GORILLA_Stage/categories.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
```
Tento proces bol zopakovaný pre všetky ďalšie tabuľky, čím sa zabezpečil import všetkých potrebných údajov.

---
### **3.2 Transformácia dát**
Transformačná fáza ETL procesu v tomto prípade spočívala v spracovaní a úprave dát z dočasných staging tabuliek do dimenzií a faktovej tabuľky. Hlavným cieľom je pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

Dimenzie boli navrhnuté na poskytovanie kontextu pre faktovú tabuľku. Každá z dimenzionálnych tabuliek obsahuje jedinečné hodnoty pre každý atribút a slúži ako referenčná tabuľka pre faktovú tabuľku. `customers_dim` obsahuje údaje ako meno zákazníka, kontaktné informácie, mesto a krajinu.

```sql
CREATE TABLE customers_dim
AS SELECT DISTINCT
    customerid as dim_customerID,
    customername as Name,
    contactname as Contact,
    city as City,
    country as Country
FROM customers_staging;
```

Dimenzia `date_dim` je navrhnutá na uchovávanie informácií o dátumoch objednávok. Obsahuje odvodené údaje, ako sú rok, mesiac, deň, deň v týždni (v textovom aj číselnom formáte), a názov mesiaca. Táto dimenzia umožňuje podrobné časové analýzy, ako sú trendy objednávok podľa dní, mesiacov alebo rokov. Pre každý záznam sa vytvára jedinečný identifikátor `date_dimID`, ktorý je generovaný pomocou funkcie ROW_NUMBER(). Dimenzia date_dim obsahuje aj názvy mesiacov a dní v týždni v slovenčine, čo umožňuje lepšiu čitateľnosť a analýzu časových dát.

```sql
CREATE TABLE date_dim
AS SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(OrderDate AS DATE)) AS dim_dateID,
    OrderDate,
    DATE_PART(year, OrderDate) AS year,
    DATE_PART(month, OrderDate) AS month,
    
    CASE DATE_PART(month, OrderDate)
        WHEN 1 THEN 'Január'
        WHEN 2 THEN 'Február'
        WHEN 3 THEN 'Marec'
        WHEN 4 THEN 'Apríl'
        WHEN 5 THEN 'Máj'
        WHEN 6 THEN 'Jún'
        WHEN 7 THEN 'Júl'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'Október'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS Month_String,
    
    DATE_PART(day, OrderDate) AS day,
    DATE_PART(dayofweek, OrderDate) + 1 AS Weekday,
    
    CASE DATE_PART(dayofweek, OrderDate) + 1
        WHEN 1 THEN 'Pondelok'
        WHEN 2 THEN 'Utorok'
        WHEN 3 THEN 'Streda'
        WHEN 4 THEN 'Štvrtok'
        WHEN 5 THEN 'Piatok'
        WHEN 6 THEN 'Sobota'
        WHEN 7 THEN 'Nedeľa'
    END AS Weekday_String

FROM orders_staging
GROUP BY OrderDate,
        DATE_PART(year, OrderDate),
        DATE_PART(month, OrderDate),
        DATE_PART(day, OrderDate),
        DATE_PART(dayofweek, OrderDate);

select * from date_dim;
```

Dimenzia `products_dim` uchováva informácie o názve produktu, jeho kategórie, jednotkách (balenie napr.: 2 x 500g, 1 ks...) a popise.

```sql
CREATE TABLE products_dim
AS SELECT DISTINCT
    productid as dim_productID,
    productname as Product,
    cs.categoryname as Category,
    unit as Unit,
    cs.description as Description
FROM products_staging ps
LEFT JOIN categories_staging cs ON ps.categoryid = cs.categoryid;
```

Dimenzia `suppliers_dim` uchováva informácie o názve dodávateľa, kontaktnú osobu a krajinu.

```sql
CREATE TABLE suppliers_dim
AS SELECT DISTINCT
    supplierid as dim_supplierID,
    suppliername as Supplier_Name,
    contactname as Contact,
    country as Country
FROM suppliers_staging;
```

Dimenzia `suppliers_dim` uchováva informácie o názve dopravcu a jeho telefónnom čísle.

```sql
CREATE TABLE shippers_dim
AS SELECT DISTINCT
    shipperid as dim_shipperID,
    shippername as Shipper_Name,
    phone as Phone
FROM shippers_staging;
```

Dimenzia `employees_dim` uchováva informácie o mene a priezvisku zamestnanca, jeho dátume narodenia a poznámkach.

```sql
CREATE TABLE employees_dim
AS SELECT DISTINCT
    employeeid as dim_employeeID,
    firstname as First_Name,
    lastname as Last_Name,
    birthdate as Birth_Date,
    notes as Notes
FROM employees_staging;
```

Faktová tabuľka `orders_facts` obsahuje záznamy o objednávkach a prepojenia na všetky dimenzie. Obsahuje kľúčové metriky, ako je množstvo, cena, dátum objednávky, identifikátory zákazníka, zamestnanca, dátumu, dopravcu, dodávateľa a produktu.

```sql
CREATE TABLE orders_facts
AS SELECT
    os.orderid as fact_orderID,
    ds.quantity as Quantity,
    ps.price as Price,
    os.orderdate as Order_Date,
    cd.dim_customerid as CustomerID,
    ed.dim_employeeid as EmployeeID,
    dd.dim_dateid as DateID,
    sd.dim_shipperid as ShipperID,
    ud.dim_supplierid as SupplierID,
    pd.dim_productid as ProductID
FROM orders_staging os
LEFT JOIN orderdetails_staging ds ON os.orderid = ds.orderid
JOIN products_staging ps ON ds.productid = ps.productid
JOIN customers_dim cd ON os.customerid = cd.dim_customerid
JOIN employees_dim ed ON os.employeeid = ed.dim_employeeid
JOIN date_dim dd ON os.orderdate = dd.orderdate
JOIN shippers_dim sd ON os.shipperid = sd.dim_shipperid
JOIN suppliers_dim ud ON ps.supplierid = ud.dim_supplierid
JOIN products_dim pd ON ps.productid = pd.dim_productid;
```

---
### **3.3 Načítanie dát**

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:

```sql
DROP TABLE IF EXISTS categories_staging;
DROP TABLE IF EXISTS customers_staging;
DROP TABLE IF EXISTS employees_staging;
DROP TABLE IF EXISTS orderdetails_staging;
DROP TABLE IF EXISTS orders_staging;
DROP TABLE IF EXISTS products_staging;
DROP TABLE IF EXISTS shippers_staging;
DROP TABLE IF EXISTS suppliers_staging;
```

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
JOIN date_dim d ON f.dateID = d.date_dimID
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
