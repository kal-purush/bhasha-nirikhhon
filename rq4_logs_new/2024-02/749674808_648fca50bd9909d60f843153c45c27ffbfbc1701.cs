namespace PolymorphicCreationWithFactory;

public class Field
{
    private readonly Location _location;
    private readonly Farmer[] _farmers = { new("Adam"), new("Bob"), new("Charlie") };

    public Field(Location location)
    {
        _location = location;
    }

    public void GrowFood(Product product)
    {
        var randomIndex = new Random().Next(_farmers.Length);
        var farmer = _farmers[randomIndex];
        
        farmer.CreateFoodSourceIn(_location);
        farmer.Harvest(product);
        farmer.Send(product);
    }
}

internal record Farmer
{
    private readonly string _name;

    public Farmer(string name)
    {
        _name = name;
    }

    public void CreateFoodSourceIn(Location location) => Console.WriteLine($"{_name} plants a field in {location}.");
    public void Harvest(Product product) => Console.WriteLine($"{_name} harvests {product} from field.");
    public void Send(Product product) => Console.WriteLine($"{_name} sends {product} to the factory.");
}

public record Product(string Name);

public record Location(string Name);