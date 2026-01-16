namespace DEV_5
{
    /// <summary>
    /// This command is engaged in a call of a method of creation of new cars, in the chosen catalog.
    /// </summary>
    public class Add : ICatalogCommand
    {
        private readonly Catalog catalog;
        private string Brand { get; set; }
        private string Model { get; set; }
        private int NumberOfCars { get; set; }
        private double Price { get; set; }
        
        public Add(Catalog receivedCatalog, string brand, string model, int numberOfCars, double price)
        {
            catalog = receivedCatalog;
            Brand = brand;
            Model = model;
            NumberOfCars = numberOfCars;
            Price = price;
        }
        
        public void Execute()
        {
            catalog.AddCar(this,Brand,Model,NumberOfCars,Price);
        }
    }
}