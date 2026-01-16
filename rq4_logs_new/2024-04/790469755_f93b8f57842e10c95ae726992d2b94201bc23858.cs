namespace StarWars.API.Models.Imports;

public class StarshipImport
{
    public string Message { get; set; }
    public List<StarshipResult> Results { get; set; }
}

public class StarshipResult
{
    public string Model { get; set; }
    public string Name { get; set; }
    public string Manufacturer { get; set; }
    public string Cost_In_Credits { get; set; }
    public string Length { get; set; }
    public string Max_Atmosphering_Speed { get; set; }
    public string Crew { get; set; }
    public string Passengers { get; set; }
    public string Cargo_Capacity { get; set; }
    public string Consumables { get; set; }
    public string Hyperdrive_Rating { get; set; }
    public string MGLT { get; set; }
    public string Starship_Class { get; set; }
    public List<string> Characters { get; set; }
    public List<string> Movies { get; set; }

    public StarshipModel ConvertToModel()
    {
        return new StarshipModel
        {
            Model = Model,
            Name = Name,
            Manufacturer = Manufacturer,
            CostInCredits = Cost_In_Credits,
            Length = Length,
            MaxAtmospheringSpeed = Max_Atmosphering_Speed,
            Crew = Crew,
            CargoCapacity = Cargo_Capacity,
            Consumables = Consumables,
            HyperdriveRating = Hyperdrive_Rating,
            MGLT = MGLT,
            StarshipClass = Starship_Class
        };
    }
}