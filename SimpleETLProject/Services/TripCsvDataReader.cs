using System.Data;
using System.Globalization;
using Microsoft.VisualBasic.FileIO;
using SimpleETLProject.Models;

namespace SimpleETLProject.Services;

public sealed class TripCsvDataReader : IDataReader
{
    private readonly TextFieldParser _parser;
    private readonly TimeZoneInfo _easternTimeZone;
    private readonly int _idxPickup;
    private readonly int _idxDropoff;
    private readonly int _idxPassenger;
    private readonly int _idxDistance;
    private readonly int _idxStoreFwd;
    private readonly int _idxPuLocation;
    private readonly int _idxDoLocation;
    private readonly int _idxFareAmount;
    private readonly int _idxTipAmount;

    private TripRecord? _current;
    private bool _isClosed;

    public TripCsvDataReader(TextFieldParser parser, TimeZoneInfo easternTimeZone)
    {
        _parser = parser;
        _easternTimeZone = easternTimeZone;

        var header = _parser.ReadFields();
        if (header == null)
            throw new InvalidOperationException("CSV file appears to be empty.");

        _idxPickup = Array.IndexOf(header, "tpep_pickup_datetime");
        _idxDropoff = Array.IndexOf(header, "tpep_dropoff_datetime");
        _idxPassenger = Array.IndexOf(header, "passenger_count");
        _idxDistance = Array.IndexOf(header, "trip_distance");
        _idxStoreFwd = Array.IndexOf(header, "store_and_fwd_flag");
        _idxPuLocation = Array.IndexOf(header, "PULocationID");
        _idxDoLocation = Array.IndexOf(header, "DOLocationID");
        _idxFareAmount = Array.IndexOf(header, "fare_amount");
        _idxTipAmount = Array.IndexOf(header, "tip_amount");

        ValidateRequiredColumns(header, new[]
        {
            ("tpep_pickup_datetime", _idxPickup),
            ("tpep_dropoff_datetime", _idxDropoff),
            ("passenger_count", _idxPassenger),
            ("trip_distance", _idxDistance),
            ("store_and_fwd_flag", _idxStoreFwd),
            ("PULocationID", _idxPuLocation),
            ("DOLocationID", _idxDoLocation),
            ("fare_amount", _idxFareAmount),
            ("tip_amount", _idxTipAmount)
        });
    }

    public int FieldCount => 9; // cols we feed into Trips_Staging

    public bool Read()
    {
        if (_isClosed) return false;

        while (!_parser.EndOfData)
        {
            string[]? fields;

            try
            {
                fields = _parser.ReadFields();
            }
            catch (MalformedLineException ex)
            {
                Console.WriteLine($"Malformed CSV line, skipping: {ex.Message}");
                continue;
            }

            if (fields == null) continue;

            if (TryBuildTripRecord(fields, out var record))
            {
                _current = record;
                return true;
            }
        }

        _current = null;
        return false;
    }

    private bool TryBuildTripRecord(string[] fields, out TripRecord record)
    {
        record = new TripRecord();

        try
        {
            var pickupStr = SafeField(fields, _idxPickup);
            var dropoffStr = SafeField(fields, _idxDropoff);

            var pickupLocal = ParseDateTime(pickupStr);
            var dropoffLocal = ParseDateTime(dropoffStr);

            var pickupUtc = TimeZoneInfo.ConvertTimeToUtc(pickupLocal, _easternTimeZone);
            var dropoffUtc = TimeZoneInfo.ConvertTimeToUtc(dropoffLocal, _easternTimeZone);

            var passengerStr = SafeField(fields, _idxPassenger);
            var distanceStr = SafeField(fields, _idxDistance);
            var storeFwdStr = SafeField(fields, _idxStoreFwd);
            var puStr = SafeField(fields, _idxPuLocation);
            var doStr = SafeField(fields, _idxDoLocation);
            var fareStr = SafeField(fields, _idxFareAmount);
            var tipStr = SafeField(fields, _idxTipAmount);

            if (!byte.TryParse(passengerStr, NumberStyles.Integer, CultureInfo.InvariantCulture, out var passenger))
                passenger = 0;

            var distance = decimal.Parse(distanceStr, NumberStyles.Float, CultureInfo.InvariantCulture);
            var pu = int.Parse(puStr, NumberStyles.Integer, CultureInfo.InvariantCulture);
            var @do = int.Parse(doStr, NumberStyles.Integer, CultureInfo.InvariantCulture);
            var fare = decimal.Parse(fareStr, NumberStyles.Float, CultureInfo.InvariantCulture);
            var tip = decimal.Parse(tipStr, NumberStyles.Float, CultureInfo.InvariantCulture);

            var storeFwd = NormalizeStoreAndFwd(storeFwdStr);

            record.PickupUtc = pickupUtc;
            record.DropoffUtc = dropoffUtc;
            record.PassengerCount = passenger;
            record.TripDistance = distance;
            record.StoreAndFwdFlag = storeFwd;
            record.PULocationId = pu;
            record.DOLocationId = @do;
            record.FareAmount = fare;
            record.TipAmount = tip;

            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Skipping invalid row: {ex.Message}");
            return false;
        }
    }

    public object GetValue(int i)
    {
        if (_current == null)
            throw new InvalidOperationException("No current row. Call Read() first.");

        return i switch
        {
            0 => _current.PickupUtc,
            1 => _current.DropoffUtc,
            2 => _current.PassengerCount,
            3 => _current.TripDistance,
            4 => (object?)_current.StoreAndFwdFlag ?? DBNull.Value,
            5 => _current.PULocationId,
            6 => _current.DOLocationId,
            7 => _current.FareAmount,
            8 => _current.TipAmount,
            _ => throw new IndexOutOfRangeException()
        };
    }

    public string GetName(int i) => i switch
    {
        0 => "tpep_pickup_datetime",
        1 => "tpep_dropoff_datetime",
        2 => "passenger_count",
        3 => "trip_distance",
        4 => "store_and_fwd_flag",
        5 => "PULocationID",
        6 => "DOLocationID",
        7 => "fare_amount",
        8 => "tip_amount",
        _ => throw new IndexOutOfRangeException()
    };

    public int GetOrdinal(string name)
    {
        for (int i = 0; i < FieldCount; i++)
            if (string.Equals(GetName(i), name, StringComparison.OrdinalIgnoreCase))
                return i;
        return -1;
    }

    public Type GetFieldType(int i) => i switch
    {
        0 or 1 => typeof(DateTime),
        2 => typeof(byte),
        3 => typeof(decimal),
        4 => typeof(string),
        5 or 6 => typeof(int),
        7 or 8 => typeof(decimal),
        _ => throw new IndexOutOfRangeException()
    };

    public bool IsDBNull(int i)
    {
        if (_current == null) return true;
        if (i == 4) return _current.StoreAndFwdFlag == null;
        return false;
    }

    public int GetValues(object[] values)
    {
        for (int i = 0; i < FieldCount && i < values.Length; i++)
            values[i] = GetValue(i);
        return Math.Min(FieldCount, values.Length);
    }

    public bool NextResult() => false;
    public int Depth => 0;
    public bool IsClosed => _isClosed;
    public int RecordsAffected => -1;

    public void Close()
    {
        _isClosed = true;
        _parser.Close();
    }

    public DataTable GetSchemaTable() => throw new NotSupportedException();
    public bool GetBoolean(int i) => (bool)GetValue(i);
    public byte GetByte(int i) => (byte)GetValue(i);
    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
    public char GetChar(int i) => (char)GetValue(i);
    public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
    public IDataReader GetData(int i) => throw new NotSupportedException();
    public string GetDataTypeName(int i) => GetFieldType(i).Name;
    public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
    public decimal GetDecimal(int i) => (decimal)GetValue(i);
    public double GetDouble(int i) => Convert.ToDouble(GetValue(i), CultureInfo.InvariantCulture);
    public float GetFloat(int i) => Convert.ToSingle(GetValue(i), CultureInfo.InvariantCulture);
    public Guid GetGuid(int i) => (Guid)GetValue(i);
    public short GetInt16(int i) => Convert.ToInt16(GetValue(i), CultureInfo.InvariantCulture);
    public int GetInt32(int i) => Convert.ToInt32(GetValue(i), CultureInfo.InvariantCulture);
    public long GetInt64(int i) => Convert.ToInt64(GetValue(i), CultureInfo.InvariantCulture);
    public string GetString(int i) => Convert.ToString(GetValue(i), CultureInfo.InvariantCulture) ?? string.Empty;
    public object this[int i] => GetValue(i);
    public object this[string name] => GetValue(GetOrdinal(name));

    public void Dispose() => Close();

    private static string SafeField(string[] fields, int index)
    {
        if (index < 0 || index >= fields.Length) return string.Empty;
        return fields[index]?.Trim() ?? string.Empty;
    }

    private static void ValidateRequiredColumns(string[] headerFields, (string Name, int Index)[] required)
    {
        foreach (var (name, index) in required)
        {
            if (index < 0)
                throw new InvalidOperationException($"Required column '{name}' not found in CSV header.");
        }
    }

    private static DateTime ParseDateTime(string s)
    {
        if (DateTime.TryParseExact(
                s,
                new[]
                {
                    "MM/dd/yyyy hh:mm:ss tt",
                    "M/d/yyyy h:mm:ss tt",
                    "yyyy-MM-dd HH:mm:ss",
                    "yyyy-MM-dd HH:mm",
                },
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var dt))
        {
            return dt;
        }

        return DateTime.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.None);
    }

    private static string? NormalizeStoreAndFwd(string s)
    {
        s = s?.Trim() ?? string.Empty;
        return s switch
        {
            "Y" => "Yes",
            "N" => "No",
            "" => null,
            _ => s
        };
    }
}