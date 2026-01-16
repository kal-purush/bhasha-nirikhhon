namespace FinallChallenge.Domain.Models
{
    /// <summary>
    /// Represents a sales territory.
    /// </summary>
    public class SalesTerritory
    {
        /// <summary>
        /// Gets or sets the ID of the sales territory.
        /// </summary>
        public int TerritoryID { get; set; }

        /// <summary>
        /// Gets or sets the name of the sales territory.
        /// </summary>
        public string? Name { get; set; }
    }
}