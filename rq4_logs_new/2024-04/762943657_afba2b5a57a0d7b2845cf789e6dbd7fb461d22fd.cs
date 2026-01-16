namespace MarketVault.Core.Models
{
    /// <summary>
    /// Service model used for adding products to operations
    /// </summary>
    public class ProductForOperationServiceModel
    {
        /// <summary>
        /// Model id
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Model quantity
        /// </summary>
        public decimal Quantity { get; set; }

        /// <summary>
        /// Product sale price
        /// </summary>
        public decimal SalePrice { get; set; }

        /// <summary>
        /// Product purchase price
        /// </summary>
        public decimal PurchasePrice { get; set; }
    }
}