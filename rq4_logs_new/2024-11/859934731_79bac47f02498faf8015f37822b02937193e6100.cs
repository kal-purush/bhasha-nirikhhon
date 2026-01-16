
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;

namespace Northwind.Entities.Concrete
{
    public class ProductCategory
    {
        [Key] // Primary Key olduğunu belirtiyoruz.
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)] // Bu kolonun IDENTITY olduğunu belirtiyoruz.
        public int ProductCategoryID { get; set; }
        public int ProductID { get; set; }
        public int CategoryID { get; set; }
        public Product Product { get; set; }
        public Category Category { get; set; }
    }
}