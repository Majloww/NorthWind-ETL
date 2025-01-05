USE ROLE TRAINING_ROLE;
USE WAREHOUSE GORILLA_WH;
CREATE DATABASE IF NOT EXISTS GORILLA_NORTHWIND_DB;
USE GORILLA_NORTHWIND_DB;

CREATE SCHEMA GORILLA_NORTHWIND_DB.staging;
USE SCHEMA GORILLA_NORTHWIND_DB.staging;

CREATE OR REPLACE STAGE GORILLA_Stage;


create table categories_staging (
    CategoryID INT PRIMARY KEY,
    CategoryName VARCHAR(25),
    Description VARCHAR(255)
);


create table suppliers_staging (
    SupplierID INT PRIMARY KEY,
    SupplierName VARCHAR(50),
    ContactName VARCHAR(50),
    Address VARCHAR(50),
    City VARCHAR(20),
    PostalCode VARCHAR(10),
    Country VARCHAR(15),
    Phone VARCHAR(15)
);


CREATE TABLE products_staging (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(50),
    SupplierID INT,
    CategoryID INT,
    Unit VARCHAR(25),
    Price DECIMAL(10,0),
    FOREIGN KEY (CategoryID) REFERENCES categories_staging(CategoryID),
    FOREIGN KEY (SupplierID) REFERENCES suppliers_staging(SupplierID)
);


create table shippers_staging (
    ShipperID INT PRIMARY KEY,
    ShipperName VARCHAR(25),
    Phone VARCHAR(15)
);


create table customers_staging (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(50),
    ContactName VARCHAR(50),
    Address VARCHAR(50),
    City VARCHAR(20),
    PostalCode VARCHAR(10),
    Country VARCHAR(15)
);


create table employees_staging (
    EmployeeID INT PRIMARY KEY,
    LastName VARCHAR(15),
    FirstName VARCHAR(15),
    BirthDate DATETIME,
    Photo VARCHAR(25),
    Notes VARCHAR(1024)
);


create table orders_staging (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    EmployeeID INT,
    OrderDate DATETIME,
    ShipperID INT,
    FOREIGN KEY (CustomerID) REFERENCES customers_staging(CustomerID),
    FOREIGN KEY (EmployeeID) REFERENCES employees_staging(EmployeeID),
    FOREIGN KEY (ShipperID) REFERENCES shippers_staging(ShipperID)
);


create table orderdetails_staging (
    OrderDetailID INT PRIMARY KEY,
    OrderID INT,
    ProductID INT,
    Quantity INT,
    FOREIGN KEY (OrderID) REFERENCES orders_staging(OrderID),
    FOREIGN KEY (ProductID) REFERENCES products_staging(ProductID)
);



COPY INTO categories_staging
FROM @GORILLA_Stage/categories.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO customers_staging
FROM @GORILLA_Stage/customers.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO employees_staging
FROM @GORILLA_Stage/employees.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO orderdetails_staging
FROM @GORILLA_Stage/orderdetails.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO orders_staging
FROM @GORILLA_Stage/orders.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO products_staging
FROM @GORILLA_Stage/products.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO shippers_staging
FROM @GORILLA_Stage/shippers.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


COPY INTO suppliers_staging
FROM @GORILLA_Stage/suppliers.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


CREATE TABLE customers_dim
AS SELECT DISTINCT
    customerid as dim_customerID,
    customername as Name,
    contactname as Contact,
    city as City,
    country as Country
FROM customers_staging;


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


CREATE TABLE products_dim
AS SELECT DISTINCT
    productid as dim_productID,
    productname as Product,
    cs.categoryname as Category,
    unit as Unit,
    cs.description as Description
FROM products_staging ps
LEFT JOIN categories_staging cs ON ps.categoryid = cs.categoryid;


CREATE TABLE suppliers_dim
AS SELECT DISTINCT
    supplierid as dim_supplierID,
    suppliername as Supplier_Name,
    contactname as Contact,
    country as Country
FROM suppliers_staging;


CREATE TABLE shippers_dim
AS SELECT DISTINCT
    shipperid as dim_shipperID,
    shippername as Shipper_Name,
    phone as Phone
FROM shippers_staging;


CREATE TABLE employees_dim
AS SELECT DISTINCT
    employeeid as dim_employeeID,
    firstname as First_Name,
    lastname as Last_Name,
    birthdate as Birth_Date,
    notes as Notes
FROM employees_staging;


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



DROP TABLE IF EXISTS categories_staging;
DROP TABLE IF EXISTS customers_staging;
DROP TABLE IF EXISTS employees_staging;
DROP TABLE IF EXISTS orderdetails_staging;
DROP TABLE IF EXISTS orders_staging;
DROP TABLE IF EXISTS products_staging;
DROP TABLE IF EXISTS shippers_staging;
DROP TABLE IF EXISTS suppliers_staging;
