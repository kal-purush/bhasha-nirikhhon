using System.Reflection;

namespace BrassAndPoem.Tests;

public class BrassAndPoemTests
{
    private List<Product> Products => new List<Product>
            {
                new Product {Name = "Trumpet", Price = 150.99M, ProductTypeId = 1},
                new Product {Name = "Trombone", Price = 246.99M, ProductTypeId = 1},
                new Product {Name = "Tuba", Price = 1250.99M, ProductTypeId = 1},
                new Product {Name = "Ozymandias", Price = 12350.99M, ProductTypeId = 2},
                new Product {Name = "Leaves of Grass", Price = 15650.99M, ProductTypeId = 2}
            };

    private List<ProductType> ProductTypes => new List<ProductType>
            {
                new ProductType {Id = 1, Title = "Brass"},
                new ProductType {Id = 2, Title = "Poem"}
            };

    [Fact]
    public void TestAddProduct()
    {
        var addProductMethod = GetMethodFromTopLevel("AddProduct");
        var products = Products;
        var productTypes = ProductTypes;
        using (var reader = new StringReader(@"The Raven
14900.05
1"))
        {
            //input values for the add product dialog
            Console.SetIn(reader);

            addProductMethod.Invoke(null, new object[] { products, productTypes });
            //there should now be one more product
            Assert.True(products.Count == 6);
            //the new product should be called the Raven
            var addedProduct = products.FirstOrDefault(p => p.Name == "The Raven" && p.Price == 14900.05M && p.ProductTypeId == 1);
            Assert.False(addedProduct == null);
        }
}

    [Fact]
    public void TestDeleteProduct()
    {
        var products = Products;
        var deleteMethod = GetMethodFromTopLevel("DeleteProduct");
        using (var reader = new StringReader("1"))
        {
            Console.SetIn(reader);
            deleteMethod.Invoke(null, new object[] { products, ProductTypes });

            //the Trombone should be deleted
            Assert.Equal(products.FirstOrDefault(p => p.Name == "Trombone"), null);
            // the count should be one less
            Assert.True(products.Count == 4);
        }
    }

    [Fact]
    public void TestUpdateProduct()
    {
        var products = Products;
        var updateMethod = GetMethodFromTopLevel("UpdateProduct");
        // inputs for update dialog (just change name, leave type and price)
        using (var reader = new StringReader(@"1
French Horn


"))
        {
            Console.SetIn(reader);
            updateMethod.Invoke(null, new object[] { products, ProductTypes });

            //There should be a French Horn
            Assert.NotEqual(products.Single(p => p.Name == "French Horn"), null);
            //There should not be a Trombone
            Assert.Equal(products.FirstOrDefault(p => p.Name == "Trombone"), null);
            //The total number should still be 5
            Assert.Equal(products.Count, 5);
        }
    }

    [Fact]
    public void TestDisplayAllProducts()
    {
        var displayMethod = GetMethodFromTopLevel("DisplayAllProducts");
        using (var writer = new StringWriter())
        {
            Console.SetOut(writer);
            displayMethod.Invoke(null, new object[] { Products, ProductTypes });
            var output = writer.ToString();
            Assert.True(
                output.Contains("Brass") &&
                output.Contains("Poem") &&
                output.Contains("Trombone") &&
                output.Contains("Trumpet") &&
                output.Contains("Tuba") &&
                output.Contains("Ozymandias") &&
                output.Contains("Leaves of Grass") &&
                output.Contains("150.99") &&
                output.Contains("246.99") &&
                output.Contains("1250.99") &&
                output.Contains("12350.99") &&
                output.Contains("15650.99")
                );
        }
    }

    [Fact]
    public void TestDisplayMenu()
    {
        var displayMethod = GetMethodFromTopLevel("DisplayMenu");
        using (var writer = new StringWriter())
        {
            Console.SetOut(writer);
            displayMethod.Invoke(null, new object[] { });
            Assert.Equal(writer.ToString(), @"1. Display all products
2. Delete a product
3. Add a new product
4. Update product properties
5. Exit
");
        }

    }

    private MethodInfo GetMethodFromTopLevel(string methodName)
    {
        var program = typeof(Program).GetTypeInfo();
        var method = program.DeclaredMethods.Single(m => m.Name.Contains(methodName)); ;

        return method;
    }
}