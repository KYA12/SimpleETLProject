using System.Globalization;
using System.Runtime.InteropServices;
using Microsoft.Data.SqlClient;
using Microsoft.VisualBasic.FileIO;

namespace SimpleETLProject.Services;

public class TripEtlService
{
    private readonly string _connectionString;

    public TripEtlService(string connectionString)
    {
        _connectionString = connectionString;
    }

    public void Run(string inputCsvPath, string duplicatesCsvPath)
    {
        Console.WriteLine("Starting ETL (10GB-friendly mode)...");

        if (!File.Exists(inputCsvPath))
        {
            Console.WriteLine($"Input file not found: {Path.GetFullPath(inputCsvPath)}");
            return;
        }

        var easternTimeZone = GetEasternTimeZone();

        // Bulk load CSV -> Trips_Staging via streaming IDataReader
        using (var parser = new TextFieldParser(inputCsvPath)
        {
            TextFieldType = FieldType.Delimited,
            HasFieldsEnclosedInQuotes = true
        })
        {
            parser.SetDelimiters(",");

            using var reader = new TripCsvDataReader(parser, easternTimeZone);
            using var connection = new SqlConnection(_connectionString);
            connection.Open();

            using var bulkCopy = new SqlBulkCopy(
                connection,
                SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.CheckConstraints,
                null)
            {
                DestinationTableName = "dbo.Trips_Staging",
                BulkCopyTimeout = 0 // unlimited; tune as needed
            };

            bulkCopy.ColumnMappings.Add("tpep_pickup_datetime", "tpep_pickup_datetime");
            bulkCopy.ColumnMappings.Add("tpep_dropoff_datetime", "tpep_dropoff_datetime");
            bulkCopy.ColumnMappings.Add("passenger_count", "passenger_count");
            bulkCopy.ColumnMappings.Add("trip_distance", "trip_distance");
            bulkCopy.ColumnMappings.Add("store_and_fwd_flag", "store_and_fwd_flag");
            bulkCopy.ColumnMappings.Add("PULocationID", "PULocationID");
            bulkCopy.ColumnMappings.Add("DOLocationID", "DOLocationID");
            bulkCopy.ColumnMappings.Add("fare_amount", "fare_amount");
            bulkCopy.ColumnMappings.Add("tip_amount", "tip_amount");

            Console.WriteLine("Bulk inserting into dbo.Trips_Staging...");
            bulkCopy.WriteToServer(reader);
            Console.WriteLine("Bulk insert into staging complete.");
        }

        // Deduplicate in SQL (staging -> Trips + Trips_Duplicates)
        DedupeInDatabase();

        // Export Trips_Duplicates to duplicates.csv
        ExportDuplicatesToCsv(duplicatesCsvPath);

        // Show final row count
        PrintRowCountFromDatabase();
    }

    private void DedupeInDatabase()
    {
        const string sql = @"
BEGIN TRAN;

;WITH cte AS (
    SELECT
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        store_and_fwd_flag,
        PULocationID,
        DOLocationID,
        fare_amount,
        tip_amount,
        ROW_NUMBER() OVER (
            PARTITION BY tpep_pickup_datetime,
                         tpep_dropoff_datetime,
                         passenger_count
            ORDER BY (SELECT 0)
        ) AS rn
    FROM dbo.Trips_Staging
)
INSERT INTO dbo.Trips (
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    fare_amount,
    tip_amount
)
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    fare_amount,
    tip_amount
FROM cte
WHERE rn = 1;

;WITH cte AS (
    SELECT
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        store_and_fwd_flag,
        PULocationID,
        DOLocationID,
        fare_amount,
        tip_amount,
        ROW_NUMBER() OVER (
            PARTITION BY tpep_pickup_datetime,
                         tpep_dropoff_datetime,
                         passenger_count
            ORDER BY (SELECT 0)
        ) AS rn
    FROM dbo.Trips_Staging
)
INSERT INTO dbo.Trips_Duplicates (
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    fare_amount,
    tip_amount
)
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    fare_amount,
    tip_amount
FROM cte
WHERE rn > 1;

DELETE FROM dbo.Trips_Staging;

COMMIT;
";

        Console.WriteLine("Running deduplication SQL...");
        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        using var cmd = new SqlCommand(sql, conn)
        {
            CommandTimeout = 0
        };
        cmd.ExecuteNonQuery();
        Console.WriteLine("Deduplication complete.");
    }

    private void ExportDuplicatesToCsv(string duplicatesCsvPath)
    {
        Console.WriteLine("Exporting duplicates to CSV...");

        if (File.Exists(duplicatesCsvPath))
        {
            File.SetAttributes(duplicatesCsvPath, FileAttributes.Normal);
        }

        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        using var cmd = new SqlCommand(@"
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    fare_amount,
    tip_amount
FROM dbo.Trips_Duplicates;
", conn);

        using var reader = cmd.ExecuteReader();
        using var writer = new StreamWriter(duplicatesCsvPath);

        // header
        writer.WriteLine("tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,store_and_fwd_flag,PULocationID,DOLocationID,fare_amount,tip_amount");

        while (reader.Read())
        {
            var fields = new string[9];

            fields[0] = ((DateTime)reader["tpep_pickup_datetime"]).ToString("o", CultureInfo.InvariantCulture);
            fields[1] = ((DateTime)reader["tpep_dropoff_datetime"]).ToString("o", CultureInfo.InvariantCulture);
            fields[2] = reader["passenger_count"].ToString() ?? "";
            fields[3] = Convert.ToString(reader["trip_distance"], CultureInfo.InvariantCulture) ?? "";
            fields[4] = reader["store_and_fwd_flag"] == DBNull.Value
                ? ""
                : (reader["store_and_fwd_flag"]?.ToString() ?? "");
            fields[5] = reader["PULocationID"].ToString() ?? "";
            fields[6] = reader["DOLocationID"].ToString() ?? "";
            fields[7] = Convert.ToString(reader["fare_amount"], CultureInfo.InvariantCulture) ?? "";
            fields[8] = Convert.ToString(reader["tip_amount"], CultureInfo.InvariantCulture) ?? "";

            writer.WriteLine(ToCsvLine(fields));
        }

        Console.WriteLine($"Duplicates exported to: {Path.GetFullPath(duplicatesCsvPath)}");
    }

    private static string ToCsvLine(string[] fields)
    {
        var output = new List<string>(fields.Length);

        foreach (var field in fields)
        {
            var value = field ?? string.Empty;
            bool mustQuote = value.Contains(",") ||
                             value.Contains("\"") ||
                             value.Contains("\n");

            if (value.Contains("\""))
            {
                value = value.Replace("\"", "\"\"");
            }

            if (mustQuote)
            {
                value = $"\"{value}\"";
            }

            output.Add(value);
        }

        return string.Join(",", output);
    }

    private void PrintRowCountFromDatabase()
    {
        try
        {
            using var conn = new SqlConnection(_connectionString);
            conn.Open();

            using var cmd = new SqlCommand("SELECT COUNT(*) FROM dbo.Trips;", conn);
            var result = cmd.ExecuteScalar();
            var count = Convert.ToInt64(result);   // <-- safe for int or long

            Console.WriteLine($"Row count in dbo.Trips: {count}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Could not read row count from database: {ex.Message}");
        }
    }

    private static TimeZoneInfo GetEasternTimeZone()
    {
        try
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");

            return TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
        }
        catch
        {
            return TimeZoneInfo.Utc;
        }
    }

    // Find out which PULocationID has the highest average tip_amount.
    public void ShowPULocationWithHighestAverageTip()
    {
        Console.WriteLine();
        Console.WriteLine("Query: PULocationID with highest average tip_amount...");

        const string sql = @"
SELECT TOP 1
    PULocationID,
    AVG(tip_amount) AS AvgTip
FROM dbo.Trips
GROUP BY PULocationID
ORDER BY AvgTip DESC;
";

        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        using var cmd = new SqlCommand(sql, conn);
        using var reader = cmd.ExecuteReader();

        if (reader.Read())
        {
            var puLocationId = (int)reader["PULocationID"];
            var avgTip = Convert.ToDecimal(reader["AvgTip"], CultureInfo.InvariantCulture);

            Console.WriteLine($"PULocationID with highest average tip: {puLocationId}, AvgTip = {avgTip:F2}");
        }
        else
        {
            Console.WriteLine("No data found in dbo.Trips.");
        }
    }

    // Top 100 longest fares in terms of trip_distance.
    public void ShowTop100ByDistance()
    {
        Console.WriteLine();
        Console.WriteLine("Query: Top 100 longest fares by trip_distance...");

        const string sql = @"
SELECT TOP 100
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    PULocationID,
    DOLocationID,
    fare_amount,
    tip_amount
FROM dbo.Trips
ORDER BY trip_distance DESC;
";

        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        using var cmd = new SqlCommand(sql, conn);
        using var reader = cmd.ExecuteReader();

        Console.WriteLine("Pickup\t\t\tDropoff\t\t\tDist\tPU\tDO\tFare\tTip");

        while (reader.Read())
        {
            var pickup = (DateTime)reader["tpep_pickup_datetime"];
            var dropoff = (DateTime)reader["tpep_dropoff_datetime"];
            var distance = Convert.ToDecimal(reader["trip_distance"], CultureInfo.InvariantCulture);
            var pu = (int)reader["PULocationID"];
            var @do = (int)reader["DOLocationID"];
            var fare = Convert.ToDecimal(reader["fare_amount"], CultureInfo.InvariantCulture);
            var tip = Convert.ToDecimal(reader["tip_amount"], CultureInfo.InvariantCulture);

            Console.WriteLine(
                $"{pickup:yyyy-MM-dd HH:mm}\t{dropoff:yyyy-MM-dd HH:mm}\t{distance:F2}\t{pu}\t{@do}\t{fare:F2}\t{tip:F2}");
        }
    }

    // Top 100 longest fares in terms of time spent traveling.
    public void ShowTop100ByDuration()
    {
        Console.WriteLine();
        Console.WriteLine("Query: Top 100 longest fares by time spent traveling...");

        const string sql = @"
SELECT TOP 100
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    PULocationID,
    DOLocationID,
    fare_amount,
    tip_amount,
    DATEDIFF(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) AS TripSeconds
FROM dbo.Trips
ORDER BY TripSeconds DESC;
";

        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        using var cmd = new SqlCommand(sql, conn);
        using var reader = cmd.ExecuteReader();

        Console.WriteLine("Pickup\t\t\tDropoff\t\t\tSecs\tDist\tPU\tDO\tFare\tTip");

        while (reader.Read())
        {
            var pickup = (DateTime)reader["tpep_pickup_datetime"];
            var dropoff = (DateTime)reader["tpep_dropoff_datetime"];
            var seconds = Convert.ToInt64(reader["TripSeconds"], CultureInfo.InvariantCulture);
            var distance = Convert.ToDecimal(reader["trip_distance"], CultureInfo.InvariantCulture);
            var pu = (int)reader["PULocationID"];
            var @do = (int)reader["DOLocationID"];
            var fare = Convert.ToDecimal(reader["fare_amount"], CultureInfo.InvariantCulture);
            var tip = Convert.ToDecimal(reader["tip_amount"], CultureInfo.InvariantCulture);

            Console.WriteLine(
                $"{pickup:yyyy-MM-dd HH:mm}\t{dropoff:yyyy-MM-dd HH:mm}\t{seconds}\t{distance:F2}\t{pu}\t{@do}\t{fare:F2}\t{tip:F2}");
        }
    }

    // Search, where part of the conditions is PULocationID.
    // Here we do a simple search: PULocationID = X, latest 100 trips.
    public void SearchTripsByPULocation(int puLocationId)
    {
        Console.WriteLine();
        Console.WriteLine($"Query: Search trips where PULocationID = {puLocationId} (top 100 by pickup datetime desc)...");

        const string sql = @"
SELECT TOP 100
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    PULocationID,
    DOLocationID,
    fare_amount,
    tip_amount
FROM dbo.Trips
WHERE PULocationID = @PULocationID
ORDER BY tpep_pickup_datetime DESC;
";

        using var conn = new SqlConnection(_connectionString);
        conn.Open();

        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@PULocationID", puLocationId);

        using var reader = cmd.ExecuteReader();

        Console.WriteLine("Pickup\t\t\tDropoff\t\t\tDist\tPassengers\tPU\tDO\tFare\tTip");

        while (reader.Read())
        {
            var pickup = (DateTime)reader["tpep_pickup_datetime"];
            var dropoff = (DateTime)reader["tpep_dropoff_datetime"];
            var distance = Convert.ToDecimal(reader["trip_distance"], CultureInfo.InvariantCulture);
            var passengers = Convert.ToByte(reader["passenger_count"], CultureInfo.InvariantCulture);
            var pu = (int)reader["PULocationID"];
            var @do = (int)reader["DOLocationID"];
            var fare = Convert.ToDecimal(reader["fare_amount"], CultureInfo.InvariantCulture);
            var tip = Convert.ToDecimal(reader["tip_amount"], CultureInfo.InvariantCulture);

            Console.WriteLine(
                $"{pickup:yyyy-MM-dd HH:mm}\t{dropoff:yyyy-MM-dd HH:mm}\t{distance:F2}\t{passengers}\t\t{pu}\t{@do}\t{fare:F2}\t{tip:F2}");
        }
    }
}