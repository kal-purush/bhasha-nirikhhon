using System;
using System.Data.SqlClient;

namespace AutoAuctionProjekt.Classes.Data.Adapters; 

public static class AuctionAdapter {
    
    #region SQL Queries
    // REVIEW: Do we want to just unconditionally fetch the related data? or should we multiple views/procs and do it on demand?
    private const string GetAuctionQuery = @"
SELECT 
    Auctions.ID, 
    Auctions.AuctionStart, 
    Auctions.VehicleID, 
    Auctions.SellerID, 
    Auctions.HighestBidderID, 
    Auctions.StandingBid, 
    Auctions.MinimumBid,
    Vehicles.ID, 
    Vehicles.Name, 
    Vehicles.Kilometers, 
    Vehicles.RegistrationNumber, 
    Vehicles.Year, 
    Vehicles.NewPrice, 
    Vehicles.HasTowbar, 
    Vehicles.EngineSize, 
    Vehicles.KmPerLiter, 
    Vehicles.FuelType, 
    Vehicles.EnergyClass
    FROM Auctions
    INNER JOIN Vehicles ON Vehicles.ID = Auctions.VehicleID
    INNER JOIN Users SU ON SU.ID = Auctions.SellerID
    INNER JOIN Users BU ON BU.ID = Auctions.HighestBidderID
    ";

    private const string GetCompletedAuctionQuery = @"
SELECT 
    Auctions.ID, 
    Auctions.AuctionStart, 
    Auctions.VehicleID, 
    Auctions.SellerID, 
    Auctions.HighestBidderID, 
    Auctions.StandingBid, 
    Auctions.MinimumBid,
    CompletedAuctions.ID, 
    CompletedAuctions.AuctionEnd, 
    CompletedAuctions.BuyerID, 
    CompletedAuctions.SoldAt
    FROM Auctions
    INNER JOIN CompletedAuctions ON Auctions.ID = CompletedAuctions.ID
    INNER JOIN Vehicles ON Vehicles.ID = Auctions.VehicleID
    INNER JOIN Users SU ON SU.ID = Auctions.SellerID
    INNER JOIN Users BU ON BU.ID = CompletedAuctions.BuyerID
    ";

    #endregion
    
    public static Auction AuctionFromReader(SqlDataReader reader) {
        // REVIEW: How do we actually instantiate a plain Vehicle? I'm not sure if we actually want it to be abstract?
        // One option could be to join the vehicle table on every type of vehicle.
        // since the Id is shared, it should only return one row.
        
        // TODO: Implement this
        throw new NotImplementedException();
    }
}