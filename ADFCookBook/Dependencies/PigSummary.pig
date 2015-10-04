SalesExtractsSourceD1 = LOAD '$InputPartitionLocationD1' USING PigStorage('|') AS (OrderDate, FirstName, LastName, CompanyName, Category, ProductName, ProvenanceCode,  ProvenanceDescription, EnglishDescription, OrderQty, UnitPrice, Discount, TaxAmount, Freight, SalesOrderNumber,PurchaseOrderNumber);
SalesExtractsSourceD2 = LOAD '$InputPartitionLocationD2' USING PigStorage('|') AS (OrderDate, FirstName, LastName, CompanyName, Category, ProductName, ProvenanceCode,  ProvenanceDescription, EnglishDescription, OrderQty, UnitPrice, Discount, TaxAmount, Freight, SalesOrderNumber,PurchaseOrderNumber);
SalesExtractsSourceD3 = LOAD '$InputPartitionLocationD3' USING PigStorage('|') AS (OrderDate, FirstName, LastName, CompanyName, Category, ProductName, ProvenanceCode,  ProvenanceDescription, EnglishDescription, OrderQty, UnitPrice, Discount, TaxAmount, Freight, SalesOrderNumber,PurchaseOrderNumber);

AllSalesExtracts = union SalesExtractsSourceD1, SalesExtractsSourceD2, SalesExtractsSourceD3;

DistinctSalesExtracts = distinct AllSalesExtracts;

SalesExtracts = foreach DistinctSalesExtracts generate FirstName, LastName, CompanyName,Category, ProductName, ProvenanceCode,  ProvenanceDescription, EnglishDescription, OrderQty,UnitPrice, Discount, TaxAmount, Freight, SalesOrderNumber,PurchaseOrderNumber;

AdfDWSource = LOAD '$StageDir' USING PigStorage('|') AS (OrderDate, CompanyName, Category, OrderQty ,UnitPrice, Discount, Tax, Freight, OrderNumber,PO);

AdfDW = foreach AdfDWSource generate CompanyName, Category, OrderDate, OrderQty AS TotalQty;

JoinedData = JOIN SalesExtracts BY (CompanyName, Category) , AdfDW BY (CompanyName, Category);

TmpSummaryData = distinct JoinedData;

SummaryData = foreach TmpSummaryData generate OrderDate, AdfDW::CompanyName, FirstName, LastName, AdfDW::Category, ProductName, ProvenanceCode, ProvenanceDescription, EnglishDescription, OrderQty, TotalQty, (double)(OrderQty/TotalQty) AS OrderPercent, UnitPrice, Discount, TaxAmount, Freight, SalesOrderNumber,PurchaseOrderNumber;

rmf $SummaryDir;

STORE SummaryData INTO '$SummaryDir' USING PigStorage('|');