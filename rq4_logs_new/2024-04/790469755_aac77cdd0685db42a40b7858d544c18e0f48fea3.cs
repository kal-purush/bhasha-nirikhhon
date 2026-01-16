namespace StarWars.API.Models.Imports;

public class VehicleInport
{
    public string Message { get; set; }
    public List<VehicleResult> Results { get; set; }

}

public class VehicleResult
{
    public string Name { get; set; }
    public string Model { get; set; }
    public string Manufacturer { get; set; }
    public string? cost_in_credits { get; set; }
    public string Length { get; set; }
    public string max_atmosphering_speed { get; set; }
    public string? Crew { get; set; }
    public string? Passengers { get; set; }
    public string cargo_capacity { get; set; }
    public string Consumables { get; set; }
    public string vehicle_class { get; set; }
    public List<MovieModel> Movies { get; set; }
    public List<CharacterModel> Character { get; set; }


public VehicleModel ConvertToModel()
    {
        return new VehicleModel
        {
            Name = Name,
            Model = Model,
            Manufacturer = Manufacturer,
            CostInCredits = cost_in_credits,
            Length = Length,
            MaxSpeed = max_atmosphering_speed,
            Crew = Crew,
            Passengers = Passengers,
            CargoCapacity = cargo_capacity,
            Consumables = Consumables,
            Class = vehicle_class,
            
        };
    }
}