namespace PolymorphicCreationWithFactory;

public class Garden
{
    private readonly Location _location;
    private readonly Gardener[] _gardeners = { new("Delilah"), new("Emily"), new("Fiona") };

    public Garden(Location location)
    {
        _location = location;
    }
    
    public void GrowFood(Product product)
    {
        var randomIndex = new Random().Next(_gardeners.Length);
        var gardener = _gardeners[randomIndex];
        
        gardener.CreateFoodSourceIn(_location);
        gardener.Harvest(product);
        gardener.Send(product);
    }
}

internal record Gardener
{
    private readonly string _name;

    public Gardener(string name)
    {
        _name = name;
    }

    public void CreateFoodSourceIn(Location location) => Console.WriteLine($"{_name} plants a garden in {location}.");
    public void Harvest(Product product) => Console.WriteLine($"{_name} harvests {product} from garden.");
    public void Send(Product product) => Console.WriteLine($"{_name} sends {product} to the market.");
}