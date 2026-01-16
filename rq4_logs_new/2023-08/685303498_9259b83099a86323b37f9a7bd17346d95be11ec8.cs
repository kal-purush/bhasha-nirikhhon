public class Burger : IFood
{
    public string Name => "Burger";
    public IList<Ingredient> Ingredients => new List<Ingredient>
    {
        new Ingredient { Name = "Bun", Quantity = 2 },
        new Ingredient { Name = "Meat Patty", Quantity = 1 },
        new Ingredient { Name = "Lettuce", Quantity = 3 },
        new Ingredient { Name = "Tomato", Quantity = 2 },
        new Ingredient { Name = "Cheese", Quantity = 1 }
    };
    public int CookTime => 15;
    public int Calories => 600;
}