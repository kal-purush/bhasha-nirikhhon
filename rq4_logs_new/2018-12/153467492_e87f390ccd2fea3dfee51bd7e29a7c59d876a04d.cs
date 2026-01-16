namespace DEV_5
{
    /// <summary>
    /// This command is engaged in calling the method of calculating
    /// the average price of the chosen brand in the selected catalog.
    /// </summary>
    public class AveragePriceType : ICatalogCommand
    {
        private readonly Catalog catalog;
        private readonly string Brand;
            
        public AveragePriceType(Catalog receivedCatalog, string brand)
        {
            catalog = receivedCatalog;
            Brand = brand;
        }

        public void Execute()
        {
            catalog.AveragePrice(this, Brand);
        }
    }
}