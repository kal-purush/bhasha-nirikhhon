using Api;
using TestFramework;
using TestFramework.Products;
using Xunit;

namespace IntegrationTests.Products;

public class ProductTest : BaseTest
{
    private readonly ProductService _productService;

    public ProductTest(MorderWebApplicationFactory<Program> factory) : base(factory)
    {
        _productService = new ProductService(Client);
    }

    [Fact]
    public async Task CreateProductSuccess()
    {
        var productId = Guid.NewGuid();
        
        ServiceActionResult<Product> result = await _productService.PostProduct(productId);
        
        Assert.True(result.Response.IsSuccessStatusCode);
    }
}