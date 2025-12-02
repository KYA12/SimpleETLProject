# Simple ETL CLI (C# + SQL Server)

A small console ETL application in CLI that inserts data from a CSV into a single, flat table. While, performs basic transformations and deduplication, and exposes a few reporting queries via an interactive console menu.

The project is implemented in **C# (.NET)** and targets **SQL Server**.

---

## High-level overview

The application:

1. Reads the input CSV (`sample-cab-data.csv`) using a **streaming CSV reader** (`TextFieldParser` + custom `IDataReader`).
2. Transforms and normalizes each row on the fly (trimming text, converting flags, converting EST → UTC).
3. Performs an efficient **bulk insert** into a staging table `dbo.Trips_Staging` using `SqlBulkCopy`.
4. Deduplicates data in SQL Server using a `ROW_NUMBER()` window function and partitions by  
   `(tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count)`.
   - Unique rows are inserted into `dbo.Trips`.
   - Duplicate rows are inserted into `dbo.Trips_Duplicates` and then exported to `duplicates.csv`.
5. Prints the total row count in the final table.
6. After ETL, provides an interactive console menu to run the sample analytical queries:
   - PULocationID with highest average tip.
   - Top 100 trips by distance.
   - Top 100 trips by travel time.
   - Search trips filtered by `PULocationID`.

---

## Project structure

```text
SimpleETLProject/
  SimpleETLProject.sln
  /SimpleETLProject
      Program.cs
      /Models
          TripRecord.cs
      /Services
          TripCsvDataReader.cs
          TripEtlService.cs
      /Sql
          db.sql
      sample-cab-data.csv
      duplicates.csv        (generated output - ignored by git)
  .gitignore
  README.md
