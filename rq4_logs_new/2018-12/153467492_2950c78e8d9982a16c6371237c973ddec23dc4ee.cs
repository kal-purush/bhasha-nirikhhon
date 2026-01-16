namespace DEV_8
{
    /// <summary>
    /// Class AveragePriceCommand
    /// This command is engaged in a call of a method of calculation
    /// of the average price, in the chosen catalog.
    /// </summary>
    public sealed class AveragePriceCommand : ICatalogCommand
    {
        private readonly IReceiver Catalog;

        public AveragePriceCommand(IReceiver receivedCatalog)
        {
            Catalog = receivedCatalog;
        }

        public void Execute()
        {
            Catalog.AveragePrice();
        }
    }
}