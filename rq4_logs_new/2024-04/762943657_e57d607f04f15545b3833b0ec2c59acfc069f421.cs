namespace MarketVault.Core.Models
{
    /// <summary>
    /// Service model for statistic
    /// </summary>
    public class StatisticServiceModel
    {
        /// <summary>
        /// Service model total products
        /// </summary>
        public int TotalProducts { get; set; }

        /// <summary>
        /// Service model total addresses
        /// </summary>
        public int TotalAddresses { get; set; }

        /// <summary>
        /// Service model total banks
        /// </summary>
        public int TotalBanks { get; set; }

        /// <summary>
        /// Service model total firms
        /// </summary>
        public int TotalFirms { get; set; }

        /// <summary>
        /// Service model total item groups
        /// </summary>
        public int TotalItemGroups { get; set; }

        /// <summary>
        /// Service model total users
        /// </summary>
        public int TotalUsers { get; set; }
    }
}