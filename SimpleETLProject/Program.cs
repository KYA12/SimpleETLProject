using SimpleETLProject.Services;

namespace SimpleETLProject;

internal class Program
{
    // Update to your SQL Server instance
    private const string ConnectionString =
        "Server=KYA123;Database=SimpleETLDB;Trusted_Connection=True;TrustServerCertificate=True;";

    private static readonly string LockFilePath = Path.Combine("..", "..", "..", "etl.lock");

    // CSV and duplicates CSV in project root
    private static readonly string InputCsvPath = Path.Combine("..", "..", "..", "sample-cab-data.csv");

    private static readonly string DuplicatesCsvPath = Path.Combine("..", "..", "..", "duplicates.csv");

    static void Main(string[] args)
    {
        try
        {
            var etlService = new TripEtlService(ConnectionString);
            bool lockExists = File.Exists(LockFilePath);
            bool dbExists = etlService.DoesDatabaseExist();

            // 1) Check if etl.lock file exists
            if (lockExists && dbExists)
            {
                Console.WriteLine("ETL was already completed previously. Skipping CSV import.");
                RunMenu(new TripEtlService(ConnectionString));
                return;
            }

            // 2) Run ETL once
            etlService.Run(InputCsvPath, DuplicatesCsvPath);

            // 3) Enter menu loop
            RunMenu(etlService);
        }
        catch (Exception ex)
        {
            Console.WriteLine("ETL failed with an unexpected error:");
            Console.WriteLine(ex.Message);
        }
    }

    private static void RunMenu(TripEtlService etlService)
    {
        while (true)
        {
            Console.WriteLine();
            Console.WriteLine("Select an operation:");
            Console.WriteLine("1) Find PULocationID with highest average tip");
            Console.WriteLine("2) Top 100 longest fares by trip_distance");
            Console.WriteLine("3) Top 100 longest fares by time spent traveling");
            Console.WriteLine("4) Search trips by PULocationID");
            Console.WriteLine("0) Exit");
            Console.Write("Your choice: ");

            var choice = Console.ReadLine();

            switch (choice)
            {
                case "1":
                    etlService.ShowPULocationWithHighestAverageTip();
                    break;

                case "2":
                    etlService.ShowTop100ByDistance();
                    break;

                case "3":
                    etlService.ShowTop100ByDuration();
                    break;

                case "4":
                    Console.Write("Enter PULocationID: ");
                    var input = Console.ReadLine();
                    if (int.TryParse(input, out var puLocationId))
                    {
                        etlService.SearchTripsByPULocation(puLocationId);
                    }
                    else
                    {
                        Console.WriteLine("Invalid PULocationID.");
                    }
                    break;

                case "0":
                    Console.WriteLine("Exiting...");
                    return;

                default:
                    Console.WriteLine("Unknown option, please try again.");
                    break;
            }
        }
    }
}