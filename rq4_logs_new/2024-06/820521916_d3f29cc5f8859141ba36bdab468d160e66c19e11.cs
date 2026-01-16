// This is an example of the Abstract Factory design pattern in C#.

internal class Program
{
    private static void Main(string[] args)
    {
      BuildHouse(new SimpleHouseFactory());
      BuildHouse(new ModernHouseFactory());
      BuildHouse(new VictorianHouseFactory());
    }

    public static void BuildHouse(IHouseFactory houseFactory)
    {
      Window window = houseFactory.CreateWindow();
      Door door = houseFactory.CreateDoor();
      Wall wall = houseFactory.CreateWall();
      Console.WriteLine($"Window size: {window.Size}, Door size: {door.Size}, Wall size: {wall.Size}");
    }
}

public interface IHouseFactory
{
    Window CreateWindow();
    Door CreateDoor();
    Wall CreateWall();
}

public class Window
{
    public int Size { get; set; }
    public string Material { get; set; }
}

public class Door
{
    public int Size { get; set; }
    public string Material { get; set; }
}

public class Wall
{
    public int Size { get; set; }
    public string Material { get; set; }
}

public class SimpleHouseFactory : IHouseFactory
{
    public Window CreateWindow()
    {
        return new Window { Size = 5, Material = "Plastic" };
    }
    public Door CreateDoor()
    {
        return new Door { Size = 5, Material = "FireWood" };
    }
    public Wall CreateWall()
    {
        return new Wall { Size = 5, Material = "Brick" };
    }
}

public class ModernHouseFactory : IHouseFactory
{
    public Window CreateWindow()
    {
        return new Window { Size = 10, Material = "Glass" };
    }
    public Door CreateDoor()
    {
        return new Door { Size = 10, Material = "Wood" };
    }
    public Wall CreateWall()
    {
        return new Wall { Size = 10, Material = "Iron" };
    }
}

public class VictorianHouseFactory : IHouseFactory
{
    public Window CreateWindow()
    {
        return new Window { Size = 20, Material = "Tempered Glass" };
    }
    public Door CreateDoor()
    {
        return new Door { Size = 20, Material = "Oak" };
    }
    public Wall CreateWall()
    {
        return new Wall { Size = 20, Material = "Stone" };
    }
}