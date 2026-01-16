using Api;
using TestFramework;
using TestFramework.Categories;
using Xunit;

namespace IntegrationTests.Categories;

public class CategoryTest : BaseTest
{
    private readonly CategoryService _categoryService;

    public CategoryTest(MorderWebApplicationFactory<Program> factory) : base(factory)
    {
        _categoryService = new CategoryService(Client);
    }

    [Fact]
    public async Task CreateParentCategorySuccess()
    {
        var categoryId = Guid.NewGuid();
        
        ServiceActionResult<Category> result = await _categoryService.PostCategory(categoryId);
        
        Assert.True(result.Response.IsSuccessStatusCode);
    }

    [Fact]
    public async Task UpdateParentCategorySuccess()
    {
        var categoryId = Guid.NewGuid();
        const string newName = "new category name";
        
        ServiceActionResult<Category> result = await _categoryService.PostCategory(categoryId);
        Assert.True(result.Response.IsSuccessStatusCode);

        Category category = result.Entity;
        category.Name = newName;
        result = await _categoryService.PostCategory(category);
        
        Assert.True(result.Response.IsSuccessStatusCode);

        ServiceActionResult<ApiResult<Category>> getResult = await _categoryService.GetCategory(categoryId);

        Assert.True(getResult.Response.IsSuccessStatusCode);
        Assert.Equal(newName, getResult.Entity.Value.Name);
    }

    [Fact]
    public async Task GetAllCategories()
    {
        var categoryId = Guid.NewGuid();
        
        await _categoryService.PostCategory(categoryId);
        ServiceActionResult<ApiResult<IEnumerable<Category>>> getResult = await _categoryService.GetCategories();
        
        Assert.True(getResult.Response.IsSuccessStatusCode);
        Assert.Contains(getResult.Entity.Value, e => e.ExternalId == categoryId);
    }

    [Fact]
    public async Task GetCategoryByExternalId()
    {
        var categoryId = Guid.NewGuid();
        
        await _categoryService.PostCategory(categoryId);
        ServiceActionResult<ApiResult<Category>> getResult = await _categoryService.GetCategory(categoryId);
        
        Assert.True(getResult.Response.IsSuccessStatusCode);
    }

    [Fact]
    public async Task GetCategoryById()
    {
        var categoryId = Guid.NewGuid();
        
        ServiceActionResult<Category> result = await _categoryService.PostCategory(categoryId);
        ServiceActionResult<ApiResult<Category>> getResult = await _categoryService.GetCategory(result.Entity.ExternalId);
        getResult = await _categoryService.GetCategory(getResult.Entity.Value.Id);
        
        Assert.True(getResult.Response.IsSuccessStatusCode);
    }

    [Fact]
    public async Task CreateChildCategoryFail()
    {
        var categoryId = Guid.NewGuid();
        var parentId = Guid.NewGuid();
        
        ServiceActionResult<Category> result = await _categoryService.PostCategory(categoryId, parentId);
        
        Assert.False(result.Response.IsSuccessStatusCode);
    }

    [Fact]
    public async Task CreateChildCategorySuccess()
    {
        var parentCategoryId = Guid.NewGuid();
        var childCategoryId = Guid.NewGuid();
        
        await _categoryService.PostCategory(parentCategoryId);
        ServiceActionResult<Category> result = await _categoryService.PostCategory(childCategoryId, parentCategoryId);
        
        Assert.True(result.Response.IsSuccessStatusCode);
    }

    [Fact]
    public async Task DeleteCategoryByExternalIdFail()
    {
        var categoryId = Guid.NewGuid();
        
        ServiceActionResult<Category> result = await _categoryService.DeleteCategory(categoryId);
        
        Assert.False(result.Response.IsSuccessStatusCode);
    }

    [Fact]
    public async Task DeleteCategoryByIdFail()
    {
        ServiceActionResult<Category> result = await _categoryService.DeleteCategory(1000);
        
        Assert.False(result.Response.IsSuccessStatusCode);
    }

    [Fact]
    public async Task DeleteCategoryByExternalIdSuccess()
    {
        var categoryId = Guid.NewGuid();
        
        await _categoryService.PostCategory(categoryId);
        
        ServiceActionResult<Category> result = await _categoryService.DeleteCategory(categoryId);
        
        Assert.True(result.Response.IsSuccessStatusCode);
    }

    [Fact]
    public async Task DeleteCategoryByIdSuccess()
    {
        var categoryId = Guid.NewGuid();
        
        await _categoryService.PostCategory(categoryId);
        
        ServiceActionResult<ApiResult<Category>> getResult = await _categoryService.GetCategory(categoryId);
        ServiceActionResult<Category> result = await _categoryService.DeleteCategory(getResult.Entity.Value.Id);
        
        Assert.True(result.Response.IsSuccessStatusCode);
    }
}