namespace DEV_8
{
    public delegate void CatalogStateHandler(object sender, CatalogEventArgs e);
    
    /// <summary>
    /// Class CatalogEventArgs
    /// Data associated with changes in the status of the catalog.
    /// </summary>
    public class CatalogEventArgs
    {
        // Event data.
        public decimal ResultOfOperation { get; private set; }
        
        public CatalogEventArgs(decimal resultOfOperation)
        {
            ResultOfOperation = resultOfOperation;
        }
    }
}