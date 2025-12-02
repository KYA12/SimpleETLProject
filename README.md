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

✔ How the project satisfies the task requirements
1. Importing CSV data into MS SQL with selected columns

Only the required columns are processed:
tpep_pickup_datetime  
tpep_dropoff_datetime  
passenger_count  
trip_distance  
store_and_fwd_flag  
PULocationID  
DOLocationID  
fare_amount  
tip_amount

TripCsvDataReader reads and validates these column names from the CSV header.
Each CSV row is parsed into a TripRecord and streamed directly into SQL Server using SqlBulkCopy.

2–3. Database and schema design

The SQL script sql/create_tables.sql:
creates the database SimpleETLDB
defines the final table dbo.Trips with correct data types (DATETIME2, TINYINT, INT, DECIMAL, VARCHAR)
defines the staging table dbo.Trips_Staging
defines dbo.Trips_Duplicates for storing removed duplicate rows

4. Schema optimized for analytical queries

The application exposes analytical operations from the console via TripEtlService:
Operation	Method	SQL Optimization
PULocationID with highest average tip	ShowPULocationWithHighestAverageTip()	Index IX_Trips_PULocation_Tip
Top 100 by trip distance	ShowTop100ByDistance()	Index IX_Trips_Distance
Top 100 by travel time	ShowTop100ByDuration()	Uses DATEDIFF + clustered index
Search by PULocationID	SearchTripsByPULocation(int)	Index IX_Trips_PULocation_Tip filters on PULocationID
All reporting queries run against dbo.Trips (the deduplicated table).

5. Efficient bulk insertion

The ETL loads data into dbo.Trips_Staging using SqlBulkCopy
TripCsvDataReader implements IDataReader over TextFieldParser, streaming rows directly from the CSV
No List<T> / DataTable / buffering is used → memory footprint stays low

6. Duplicate detection and duplicates.csv

Duplicates are defined by:
tpep_pickup_datetime  
tpep_dropoff_datetime  
passenger_count
DedupeInDatabase() runs a single SQL batch:
ROW_NUMBER() partitions rows by the 3 duplicate keys
inserts rn = 1 rows into dbo.Trips
inserts rn > 1 rows into dbo.Trips_Duplicates
clears dbo.Trips_Staging
ExportDuplicatesToCsv() then exports dbo.Trips_Duplicates into duplicates.csv.

📌 For the provided dataset:

Source CSV rows	Unique	Duplicate
30,000	29,889	111

7. store_and_fwd_flag transformation

In TripCsvDataReader:
"Y" → "Yes"  
"N" → "No"  
""  → NULL
The transformed values are written to the database.

8. Trimming spaces

All textual fields pass through:

private static string SafeField(string[] fields, int index)
    => fields[index]?.Trim() ?? string.Empty;

This eliminates all leading/trailing whitespace from parsed data.

9. 10GB CSV scenario

The implementation is already designed for large datasets:
Streaming row processing
No in-memory storage of CSV rows (IDataReader → SqlBulkCopy)
Bulk insert into an unindexed staging table
Deduplication happens 100% inside SQL Server
Potential future scalability improvements:
Partitioned ETL over multiple file chunks
Parallel readers + parallel bulk copy
Table partitioning by month or year

10. EST → UTC conversion

The CSV timestamps are assumed to be in Eastern Time.
In TripCsvDataReader:
var pickupUtc  = TimeZoneInfo.ConvertTimeToUtc(pickupLocal, _easternTimeZone);
var dropoffUtc = TimeZoneInfo.ConvertTimeToUtc(dropoffLocal, _easternTimeZone);

_easternTimeZone resolves dynamically:

OS	Zone ID
Windows	"Eastern Standard Time"
Linux / macOS	"America/New_York"
fallback	UTC

All times stored in SQL are UTC.

Handling unsafe / dirty data

Malformed CSV rows are caught via MalformedLineException and skipped
Parsing failures inside TryBuildTripRecord are logged and skipped rather than crashing the ETL
All SQL queries with user input use parameterized commands → no SQL injection risk

Row count verification

After the ETL runs, the following can be executed:

SELECT COUNT(*) FROM dbo.Trips;
SELECT COUNT(*) FROM dbo.Trips_Duplicates;
SELECT COUNT(*) FROM dbo.Trips_Staging;
