namespace PolymorphicCreationWithFactory;

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