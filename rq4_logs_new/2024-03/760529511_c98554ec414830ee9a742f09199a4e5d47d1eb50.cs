using Models.EntityFramework;

namespace sza_API.Models
{
    public class Sneaker : BaseEntity
    {
        public string SupplierId { get; set; }
        public string Name { get; set; }
        public string Brand { get; set; }
        public string Description { get; set; }
        public float Price { get; set; }
        public string Condition { get; set; }
        public string Size { get; set; }
        public string SKU { get; set; }
        public string Availability {  get; set; }

    }
}