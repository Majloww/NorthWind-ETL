-- Graf 1: Zisk za každý deň
SELECT 
    dd.OrderDate AS Day,
    SUM(ofacts.Quantity * ofacts.Price) AS Total_Revenue
FROM orders_facts ofacts
JOIN date_dim dd ON ofacts.DateID = dd.dim_dateID
GROUP BY dd.OrderDate
ORDER BY Day;

-- Graf 2: Zisk podľa kategórie
SELECT 
    pd.Category AS Category_Name,
    SUM(ofacts.Quantity * ofacts.Price) AS Total_Revenue
FROM orders_facts ofacts
JOIN products_dim pd ON ofacts.ProductID = pd.dim_productID
GROUP BY pd.Category
ORDER BY Total_Revenue DESC;

-- Graf 3: Objednávky podľa krajín
SELECT 
    cd.Country,
    COUNT(orders_facts.fact_orderID) AS Order_Count
FROM orders_facts orders_facts
JOIN customers_dim cd ON orders_facts.CustomerID = cd.dim_customerID
GROUP BY cd.Country
ORDER BY Order_Count DESC;

-- Graf 4: Počet objednávok dodaných dodávateľmi
SELECT 
    sd.Shipper_Name AS Shipper,
    SUM(ofacts.Quantity) AS Total_Quantity_Shipped
FROM orders_facts ofacts
JOIN shippers_dim sd ON ofacts.ShipperID = sd.dim_shipperID
GROUP BY sd.Shipper_Name
ORDER BY Total_Quantity_Shipped DESC;

-- Graf 5: Splnené objednávky zamestnancami
SELECT 
    ed.First_Name || ' ' || ed.Last_Name AS EmployeeName,
    COUNT(DISTINCT ofacts.fact_orderID) AS Total_Orders,
FROM orders_facts ofacts
JOIN employees_dim ed ON ofacts.EmployeeID = ed.dim_employeeID
GROUP BY ed.dim_employeeID, ed.First_Name, ed.Last_Name
ORDER BY COUNT(ofacts.fact_orderid);

-- Graf 6: Zisk zamestnancov za objednávky
SELECT 
    ed.First_Name || ' ' || ed.Last_Name AS Employee_Name,
    SUM(ofacts.Quantity * ofacts.Price) AS Total_Revenue
FROM orders_facts ofacts
JOIN employees_dim ed ON ofacts.EmployeeID = ed.dim_employeeID
GROUP BY ed.First_Name, ed.Last_Name
ORDER BY Total_Revenue DESC;
