# Simple ETL CLI (C# + SQL Server)

A small console ETL application in CLI that inserts data from a CSV into a single, flat table. It performs basic transformations and deduplication, and exposes a few reporting queries via an interactive console menu.

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
```

---

### 1. Importing CSV data into MS SQL with selected columns

Only the required columns are processed:
1. tpep_pickup_datetime  
2. tpep_dropoff_datetime  
3. passenger_count  
4. trip_distance  
5. store_and_fwd_flag  
6. PULocationID  
7. DOLocationID  
8. fare_amount  
9. tip_amount

TripCsvDataReader reads and validates these column names from the CSV header.
Each CSV row is parsed into a TripRecord and streamed directly into SQL Server using SqlBulkCopy.

### 2–3. Database and schema design

The SQL script sql/create_tables.sql:
1. creates the database SimpleETLDB
2. defines the final table dbo.Trips with correct data types (DATETIME2, TINYINT, INT, DECIMAL, VARCHAR)
3. defines the staging table dbo.Trips_Staging
4. defines dbo.Trips_Duplicates for storing removed duplicate rows.

### 4. Schema optimized for analytical queries

The application exposes analytical operations from the console via `TripEtlService`:

| Operation                                | Method                                   | SQL Optimization                                      |
|------------------------------------------|-------------------------------------------|--------------------------------------------------------|
| PULocationID with highest average tip    | `ShowPULocationWithHighestAverageTip()`   | Index `IX_Trips_PULocation_Tip`                        |
| Top 100 by trip distance                 | `ShowTop100ByDistance()`                  | Index `IX_Trips_Distance`                              |
| Top 100 by travel time                   | `ShowTop100ByDuration()`                  | Uses `DATEDIFF` + clustered index                      |
| Search by PULocationID                   | `SearchTripsByPULocation(int)`            | Fast lookup using `IX_Trips_PULocation_Tip` on filter  |

All reporting queries run against the **`dbo.Trips`** table (the deduplicated table).

### 5. Efficient bulk insertion

1. The ETL loads data into dbo.Trips_Staging using SqlBulkCopy.
2. TripCsvDataReader implements IDataReader over TextFieldParser, streaming rows directly from the CSV.
3. No List<T> / DataTable / buffering is used → memory footprint stays low.

### 6. Duplicate detection and duplicates.csv

Duplicates are defined by a combination of `pickup_datetime`(tpep_pickup_datetime), `dropoff_datetime`(tpep_dropoff_datetime), and `passenger_count`(passenger_count).

How deduplication is performed (database-side logic)

The C# method DedupeInDatabase() executes a single SQL batch that:

1. Reads all rows from dbo.Trips_Staging
2. Adds a ROW_NUMBER() window function:
3. ROW_NUMBER() OVER (
    PARTITION BY tpep_pickup_datetime,
                 tpep_dropoff_datetime,
                 passenger_count
    ORDER BY (SELECT 0)
) AS rn
4. PARTITION BY groups identical rows
5. rn = 1 -> Treated as the first (unique) occurrence -> Inserted into **dbo.Trips** (final table)
6. rn > 1 -> Considered an additional duplicate occurrence -> Inserted into **dbo.Trips_Duplicates** (dedupe log)
7. After inserts -> Cleanup of staging data -> All rows removed from **dbo.Trips_Staging**

After this process:
1. dbo.Trips contains only unique trip records
2. dbo.Trips_Duplicates contains every extra occurrence
3. dbo.Trips_Staging becomes empty and ready for next ETL run

ExportDuplicatesToCsv() is used to export dbo.Trips_Duplicates into duplicates.csv.

### For the provided dataset

| Source CSV rows | Unique rows in `dbo.Trips` | Duplicate rows in `dbo.Trips_Duplicates` |
|-----------------|----------------------------|-------------------------------------------|
| 30,000          | 29,889                     | 111                                       |

### 7. store_and_fwd_flag transformation

In TripCsvDataReader:
1. "Y" → "Yes"  
2. "N" → "No"  
3. ""  → NULL
The transformed values are written to the database.

### 8. Trimming spaces

All textual fields pass through:
private static string SafeField(string[] fields, int index)
    => fields[index]?.Trim() ?? string.Empty;
    
This eliminates all leading/trailing whitespace from parsed data.

### 9. 10GB CSV scenario

The implementation is already designed for large datasets:

1. Streaming row processing
TripCsvDataReader reads rows one at a time, keeping memory usage constant regardless of file size.
2. No CSV buffering in memory
Data flows directly from the CSV → IDataReader → SqlBulkCopy (no List<T> / DataTable>).
3. Bulk insert into an unindexed staging table
Staging table avoids index overhead during ingestion for optimal throughput.
4. Deduplication performed entirely inside SQL Server
Eliminates the need for in-memory hashing or grouping in the C# layer.

#### Potential future scalability improvements

| Improvement | Description | Benefit |
|------------|-------------|---------|
| **Chunked / partitioned CSV ingestion** | Split a massive CSV into smaller segments (either physically or by streaming offsets) and process them sequentially or in parallel | Prevents extremely long-running transactions and improves resilience |
| **Parallel ETL workers** | Use multiple reader threads and multiple `SqlBulkCopy` operations writing to separate staging tables (e.g., `Trips_Staging_1`, `Trips_Staging_2`, …), then merge | Makes full use of multi-core CPU and disk I/O |
| **Incremental ETL design** | Track the latest pickup time loaded and process only new rows | Removes need for full dataset reload |

### 10. EST → UTC conversion

The CSV timestamps are assumed to be in Eastern Time.

In TripCsvDataReader:
var pickupUtc  = TimeZoneInfo.ConvertTimeToUtc(pickupLocal, _easternTimeZone);
var dropoffUtc = TimeZoneInfo.ConvertTimeToUtc(dropoffLocal, _easternTimeZone);

All times stored in SQL are UTC.

### Handling unsafe / dirty data

1. Malformed CSV rows are caught via MalformedLineException and skipped.
2. Parsing failures inside TryBuildTripRecord are logged and skipped rather than crashing the ETL.
3. All SQL queries with user input use parameterized commands → no SQL injection risk.

### Row count verification

After the ETL runs, the following can be executed:
1. SELECT COUNT(*) FROM dbo.Trips;
2. SELECT COUNT(*) FROM dbo.Trips_Duplicates;
3. SELECT COUNT(*) FROM dbo.Trips_Staging;
