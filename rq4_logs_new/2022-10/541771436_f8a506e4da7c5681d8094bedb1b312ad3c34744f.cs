using System.ComponentModel.DataAnnotations;

namespace CupsAndCakes.Models
{
    /// <summary>
    /// A simple order
    /// </summary>
    public class Order
    {
        [Key]
        public int Id { get; set; }

        /// <summary>
        /// The name of the order
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The flavor of the order
        /// </summary>
        public string Flavor { get; set; }

        /// <summary>
        /// The type of order
        /// </summary>
        public string Type { get; set; }

        /// <summary>
        /// The number of orders
        /// </summary>
        public int Quantity { get; set; }

        /// <summary>
        /// The customer's order
        /// </summary>
        public Customer Person { get; set; }
    }
}