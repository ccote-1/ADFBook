--DROP TABLE SalesExtracts;

CREATE EXTERNAL TABLE IF NOT EXISTS SalesExtracts(
 OrderDate timestamp,
 FirstName string ,
 LastName string ,
 CompanyName string ,
 Category string ,
 ProductName string ,
 ProvenanceCode smallint ,
 ProvenanceDescription string ,
 EnglishDescription string ,
 OrderQy int,
 UnitPrice decimal(12,4),
 Discount decimal(12,4),
 TaxAmount decimal(12,4),
 Freight decimal(12,4),
 SalesOrderNumber string ,
 PurchareOrderNumber string
)
PARTITIONED BY (Year INT, Month INT, Day INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'  LINES TERMINATED BY '10' 
 STORED AS TEXTFILE;

--Add 3 partitions
ALTER TABLE SalesExtracts ADD IF NOT EXISTS
PARTITION(Year = ${hiveconf:YearD1}, Month = ${hiveconf:MonthD1}, Day = ${hiveconf:DayD1})
LOCATION '${hiveconf:InputPartitionLocationD1}';

ALTER TABLE SalesExtracts ADD IF NOT EXISTS
PARTITION(Year = ${hiveconf:YearD2}, Month = ${hiveconf:MonthD2}, Day = ${hiveconf:DayD2})
LOCATION '${hiveconf:InputPartitionLocationD2}';

ALTER TABLE SalesExtracts ADD IF NOT EXISTS
PARTITION(Year = ${hiveconf:YearD3}, Month = ${hiveconf:MonthD3}, Day = ${hiveconf:DayD3})
LOCATION '${hiveconf:InputPartitionLocationD3}';

DROP TABLE ADFDWHiveTable;

CREATE EXTERNAL TABLE ADFDWHiveTable
(   
    OrderDate       Date,                               
    company         string,                                   
    category        string,  
    qtyordered      int,
    unitprice       decimal(12,4),
    discount        decimal(3,2),
    tax             decimal(12,4),
    freight         decimal(12,4),
    ordernumber     string,
    po              string                               
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE LOCATION '${hiveconf:OutputContainer}';

INSERT OVERWRITE TABLE ADFDWHiveTable
SELECT OrderDate, CompanyName, Category ,   SUM(OrderQy) AS OrderQy, AVG(UnitPrice) AS UnitPrice, SUM(Discount) AS Discount, SUM(TaxAmount) AS TaxAmount, SUM(Freight) AS Freight, 
             SalesOrderNumber, PurchareOrderNumber
FROM   SalesExtracts
WHERE Year BETWEEN ${hiveconf:YearD1} AND ${hiveconf:YearD3} AND Month BETWEEN ${hiveconf:MonthD1} AND ${hiveconf:MonthD3} AND Day BETWEEN ${hiveconf:DayD1} AND ${hiveconf:DayD3}
GROUP BY CompanyName, Category, SalesOrderNumber, PurchareOrderNumber, OrderDate;