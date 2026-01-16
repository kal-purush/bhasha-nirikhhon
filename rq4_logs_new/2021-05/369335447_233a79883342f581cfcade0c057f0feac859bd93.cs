using System;
namespace ACM.BL
{
    public class Product
    {
        public Product()
        {
        }

        public Product(int productId)
        {
            ProductId = productId;
        }

        public decimal? CurrentPrice { get; set; }
        public string ProductDescription { get; set; }
        public int ProductId { get; private set; }
        public string ProductName { get; set; }


        public bool Validate()
        {
            if (string.IsNullOrWhiteSpace(ProductName))
            {
                return false;
            }
            else if (CurrentPrice == null)
            {
                return false;
            }
            else
            {
                return true;
            }
        }

    }
}