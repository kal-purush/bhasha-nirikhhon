using System.Data.SqlClient;
using AutoAuctionProjekt.Classes.Vehicles;

namespace AutoAuctionProjekt.Classes.Data; 

public class VehicleAdapter {
    
    #region SQL Queries

    private const string VehiclesCommon = @"
Vehicles.ID,
Vehicles.Name,
Vehicles.Kilometers,
Vehicles.RegistrationNumber,
Vehicles.Year,
Vehicles.HasTowbar,
Vehicles.EngineSize,
Vehicles.KmPerLiter,
Vehicles.FuelType,
Vehicles.EnergyClass
";

    private const string HeavyVehiclesCommon = @"
HeavyVehicles.Height,
HeavyVehicles.Weight,
HeavyVehicles.Length
";

    private const string PersonalCarsCommon = @"
PersonalCar.EngineSize,
PersonalCar.Seats,
PersonalCar.TrunkHeight,
PersonalCar.TrunkWidth,
PersonalCar.TrunkDepth,
PersonalCar.DriversLicense
";
    
    private const string GetBussesQuery = $@"SELECT 
{VehiclesCommon}, 
{HeavyVehiclesCommon},
Busses.Seats,
Busses.SleepingSpaces,
Busses.HasToilet,
Busses.DriversLicense
FROM Vehicles 
INNER JOIN HeavyVehicles ON Vehicles.ID = HeavyVehicles.VehicleID
INNER JOIN Busses ON HeavyVehicles.ID = Busses.HeavyVehicleID
";
    
    private const string GetTrucksQuery = $@"
SELECT 
{VehiclesCommon},
{HeavyVehiclesCommon},
Trucks.LoadCapacity
FROM Vehicles 
INNER JOIN HeavyVehicles ON Vehicles.ID = HeavyVehicles.VehicleID
INNER JOIN Trucks ON HeavyVehicles.ID = Trucks.HeavyVehicleID
";

    private const string PrivatePersonalCarsQuery = @"
SELECT Vehicles.Name,
Vehicles.Kilometers,
Vehicles.RegistrationNumber,
Vehicles.Year,
Vehicles.NewPrice,
Vehicles.HasTowbar,
Vehicles.KmPerLiter,
Vehicles.FuelType,
PersonalCar.EngineSize,
PersonalCar.Seats,
PersonalCar.TrunkHeight,
PersonalCar.TrunkWidth,
PersonalCar.TrunkDepth,
PersonalCar.DriversLicense,
PrivatePersonalCar.HasIsofix
FROM Vehicles
INNER JOIN PersonalCar ON Vehicles.ID = PersonalCar.VehicleID
INNER JOIN PrivatePersonalCar ON PersonalCar.ID = PrivatePersonalCar.PersonalCarID
";
    
    private const string ProfessionalPersonalCarsQuery = @"
SELECT Vehicles.Name,
Vehicles.Kilometers,
Vehicles.RegistrationNumber,
Vehicles.Year,
Vehicles.NewPrice,
Vehicles.HasTowbar,
PersonalCar.EngineSize,
Vehicles.KmPerLiter,
Vehicles.FuelType,
PersonalCar.Seats,
PersonalCar.TrunkHeight,
PersonalCar.TrunkWidth,
PersonalCar.TrunkDepth,
PersonalCar.DriversLicense,
PrivatePersonalCar.HasIsofix
FROM Vehicles
INNER JOIN PersonalCar ON Vehicles.ID = PersonalCar.VehicleID
INNER JOIN PrivatePersonalCar ON PersonalCar.ID = PrivatePersonalCar.PersonalCarID
";
    #endregion

    /// <summary>
    /// Get common properties for all vehicles
    /// </summary>
    /// <param name="reader">data reader </param>
    /// <returns></returns>
    private static VehicleProps VehiclePropsFromReader(SqlDataReader reader) => new() {
            Name = (string) reader["Name"],
            Km = (double) reader["Kilometers"],
            RegistrationNumber = (string)reader["RegistrationNumber"],
            Year = (ushort) reader["Year"],
            NewPrice = (decimal) reader["NewPrice"],
            HasTowbar = (bool) reader["HasTowbar"],
            EngineSize = (double) reader["EngineSize"],
            KmPerLiter = (double) reader["KmPerLiter"],
            FuelType = (FuelTypeEnum) reader["FuelType"]
        };

    /// <summary>
    /// Construct a Bus from a reader object
    /// </summary>
    public static Bus BusFromReader(SqlDataReader reader) {
        return new(
            VehiclePropsFromReader(reader),
            new VehicleDimensionsStruct {
                Length = (double) reader["Length"],
                Height = (double) reader["Height"],
                Weight = (double) reader["Weight"]
            },
            (ushort) reader["NumberOfSeats"],
            (ushort) reader["NumberOfSleepingPlaces"],
            (bool) reader["HasToilet"]
        ) {ID = (uint) reader["ID"]};
    }

    /// <summary>
    /// Construct a Truck from a reader object
    /// </summary>
    public static Truck TruckFromReader(SqlDataReader reader) {
        return new(
            VehiclePropsFromReader(reader),
            new VehicleDimensionsStruct {
                Length = (double) reader["Length"],
                Height = (double) reader["Height"],
                Weight = (double) reader["Weight"]
            },
            (double) reader["LoadCapacity"]
        ) {ID = (uint) reader["ID"]};
    }

    /// <summary>
    /// Construct a PrivatePersonalCar from a reader object
    /// </summary>
    public static PrivatePersonalCar GetPrivatePersonalCarFromReader(SqlDataReader reader) {
        
        return new(
            VehiclePropsFromReader(reader),
            (ushort) reader["NumberOfSeats"],
            new PersonalCar.TrunkDimentionsStruct() {
                Depth = (double) reader["TrunkDepth"],
                Height = (double) reader["TrunkHeight"],
                Width = (double) reader["TrunkWidth"]
            },
            (bool) reader["HasIsofix"],
            (bool) reader["LicenseBE"] // TODO: this is actually a varchar in the database.
        ) {ID = (uint) reader["ID"]};
    }
    
    /// <summary>
    /// Construct a ProfessionalPersonalCar from a reader object
    /// </summary>
    public static ProfessionalPersonalCar GetProfessionalPersonalCarFromReader(SqlDataReader reader) {
        return new(
            VehiclePropsFromReader(reader),
            (ushort) reader["NumberOfSeats"],
            new PersonalCar.TrunkDimentionsStruct() {
                Depth = (double) reader["TrunkDepth"],
                Height = (double) reader["TrunkHeight"],
                Width = (double) reader["TrunkWidth"]
            },
            (bool) reader["LicenseBE"], // TODO: this is actually a varchar in the database.
            (double) reader["LoadCapacity"],
            (bool) reader["HasSafetyBar"]
        ) {ID = (uint) reader["ID"]};
    }
}