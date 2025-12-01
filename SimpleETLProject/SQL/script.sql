/* 0. Create database */
IF DB_ID('SimpleETLDB') IS NULL
BEGIN
    PRINT 'Creating database SimpleETLDB...';
    CREATE DATABASE SimpleETLDB;
END
GO

USE SimpleETLDB;
GO

/* 1. Drop tables if re-running */
IF OBJECT_ID('dbo.Trips',           'U') IS NOT NULL DROP TABLE dbo.Trips;
IF OBJECT_ID('dbo.Trips_Staging',   'U') IS NOT NULL DROP TABLE dbo.Trips_Staging;
IF OBJECT_ID('dbo.Trips_Duplicates','U') IS NOT NULL DROP TABLE dbo.Trips_Duplicates;
GO

/* 2. Final table (deduplicated result) */
CREATE TABLE dbo.Trips
(
    Id                        BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    tpep_pickup_datetime      DATETIME2(0) NOT NULL,
    tpep_dropoff_datetime     DATETIME2(0) NOT NULL,
    passenger_count           TINYINT      NOT NULL,
    trip_distance             DECIMAL(9,2) NOT NULL,
    store_and_fwd_flag        VARCHAR(3)   NULL,      -- 'Yes' / 'No' / NULL
    PULocationID              INT          NOT NULL,
    DOLocationID              INT          NOT NULL,
    fare_amount               DECIMAL(10,2) NOT NULL,
    tip_amount                DECIMAL(10,2) NOT NULL
);
GO


/* 3. Staging table for bulk load (no indexes) */
CREATE TABLE dbo.Trips_Staging
(
    tpep_pickup_datetime      DATETIME2(0) NOT NULL,
    tpep_dropoff_datetime     DATETIME2(0) NOT NULL,
    passenger_count           TINYINT      NOT NULL,
    trip_distance             DECIMAL(9,2) NOT NULL,
    store_and_fwd_flag        VARCHAR(3)   NULL,
    PULocationID              INT          NOT NULL,
    DOLocationID              INT          NOT NULL,
    fare_amount               DECIMAL(10,2) NOT NULL,
    tip_amount                DECIMAL(10,2) NOT NULL
);
GO

/* 4. Table for duplicates (for export to duplicates.csv) */
CREATE TABLE dbo.Trips_Duplicates
(
    tpep_pickup_datetime      DATETIME2(0) NOT NULL,
    tpep_dropoff_datetime     DATETIME2(0) NOT NULL,
    passenger_count           TINYINT      NOT NULL,
    trip_distance             DECIMAL(9,2) NOT NULL,
    store_and_fwd_flag        VARCHAR(3)   NULL,
    PULocationID              INT          NOT NULL,
    DOLocationID              INT          NOT NULL,
    fare_amount               DECIMAL(10,2) NOT NULL,
    tip_amount                DECIMAL(10,2) NOT NULL
);
GO


/* 5. Indexes for reporting on final table Trips */

/* Avg tip by PULocationID + searches by PULocationID */
CREATE NONCLUSTERED INDEX IX_Trips_PULocation_Tip
ON dbo.Trips (PULocationID)
INCLUDE (tip_amount, trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime);
GO

/* Top longest fares by distance */
CREATE NONCLUSTERED INDEX IX_Trips_Distance
ON dbo.Trips (trip_distance DESC);
GO

/* Time-range queries by pickup datetime */
CREATE NONCLUSTERED INDEX IX_Trips_PickupTime
ON dbo.Trips (tpep_pickup_datetime);
GO

/*  Highest average tip by pickup location
SELECT TOP 1
    PULocationID,
    AVG(tip_amount) AS AvgTip
FROM dbo.Trips
GROUP BY PULocationID
ORDER BY AvgTip DESC;
*/

/* Top 100 longest fares by distance */
/*
SELECT TOP 100 *
FROM dbo.Trips
ORDER BY trip_distance DESC;
*/

/*  Top 100 longest fares by travel time */
/*
SELECT TOP 100
    *,
    DATEDIFF(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) AS TripSeconds
FROM dbo.Trips
ORDER BY TripSeconds DESC;
*/

/* Row count after ETL */
/*
SELECT COUNT(*) AS TotalRows
FROM dbo.Trips;
*/