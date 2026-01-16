namespace MarketVault.Models.Operation
{
    /// <summary>
    /// Operation view model class
    /// </summary>
    public class OperationViewModel
    {
        /// <summary>
        /// View model id
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// View model document type
        /// </summary>
        public string DocumentType { get; set; } = string.Empty;

        /// <summary>
        /// View model counter party
        /// </summary>
        public string CounterParty { get; set; } = string.Empty;

        /// <summary>
        /// View model products count
        /// </summary>
        public int ProductsCount { get; set; }

        /// <summary>
        /// View model total sale price without VAT
        /// </summary>
        public decimal TotalSalePriceWithoutVAT { get; set; }

        /// <summary>
        /// View model total sale price with VAT
        /// </summary>
        public decimal TotalSalePriceWithVAT { get; set; }

        /// <summary>
        /// View model total purchase price without VAT
        /// </summary>
        public decimal TotalPurchasePriceWithoutVAT { get; set; }

        /// <summary>
        /// View model total purchase price with VAT
        /// </summary>
        public decimal TotalPurchasePriceWithVAT { get; set; }

        /// <summary>
        /// View model date time when made
        /// </summary>
        public DateTime DateMade { get; set; } = DateTime.Now;

        /// <summary>
        /// View model creator id
        /// </summary>
        public string UserId { get; set; } = string.Empty;
    }
}