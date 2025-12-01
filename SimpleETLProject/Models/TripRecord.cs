namespace SimpleETLProject.Models;

public class TripRecord
{
    public DateTime PickupUtc { get; set; }
    public DateTime DropoffUtc { get; set; }
    public byte PassengerCount { get; set; }
    public decimal TripDistance { get; set; }
    public string? StoreAndFwdFlag { get; set; } // "Yes" / "No" / null
    public int PULocationId { get; set; }
    public int DOLocationId { get; set; }
    public decimal FareAmount { get; set; }
    public decimal TipAmount { get; set; }
}