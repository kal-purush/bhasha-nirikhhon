using ProductAPI.Modules;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProductAPI.Test.Fixtures
{
    public class TestFixtures
    {
        public product_details GetProductDetails(int id)
        {
            return new product_details()
            {
                created_on = DateTime.Now,
                product_category_code = 101,
                product_code = Guid.NewGuid(),
                product_company = "TU",
                product_id = id,
                product_description = "test desc",
                product_name = "Product Name"
            };
        }
    }
}