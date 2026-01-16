using System;
namespace ACM.BL.Repositories
{
    public class ProductRepository
    {
        public Product Retrieve(int productId)
        {
            Product product = new Product(productId);

            // HARD CODING DATA -- NO DATA ACCESS LAYER IN THIS PROJECT
            if (productId == 2)
            {
                product.CurrentPrice = 20;
                product.ProductDescription = "silky smooth";
                product.ProductName = "Shoe Laces";
            }
            return product;
        }

        public bool Save(Product product)
        {
            // HARD CODED DATA
            // NO DATA ACCESS LAYER IN THIS PROJECT
            return true;
        }
    }
}