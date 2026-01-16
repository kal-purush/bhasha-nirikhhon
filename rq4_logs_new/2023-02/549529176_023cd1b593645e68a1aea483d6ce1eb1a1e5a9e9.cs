using Answer.King.Domain.Repositories.Models;
using Answer.King.Test.Common.CustomTraits;
using Xunit;
using Product = Answer.King.Domain.Repositories.Models.Product;
using ProductCategory = Answer.King.Domain.Repositories.Models.ProductCategory;
using TagId = Answer.King.Domain.Repositories.Models.TagId;

namespace Answer.King.Domain.UnitTests.Repositories.Models;

[TestCategory(TestType.Unit)]
public class ProductTests
{
    [Fact]
    public void Product_IsRetired_AddTag_ThrowsProductLifecycleException()
    {
        // Arrange
        const string name = "name";
        const string description = "description";
        const int price = 142;
        var productCategory = new ProductCategory(1, "category name", "category description");
        var product = new Product(
            name,
            description,
            price,
            productCategory);
        var tagId = new TagId(1);
        product.Retire();

        // Act/Assert
        Assert.Throws<ProductLifecycleException>(() => product.AddTag(tagId));
    }

    [Fact]
    public void Product_IsRetired_ExistingTagAdded_ProductUnchanged()
    {
        // Arrange
        const string name = "name";
        const string description = "description";
        const int price = 142;
        var productCategory = new ProductCategory(1, "category name", "category description");
        var product = new Product(
            name,
            description,
            price,
            productCategory);
        var tagId = new TagId(1);
        product.AddTag(tagId);
        var expectedTags = new string(product.Tags.ToString());
        product.Retire();

        // Act
        product.AddTag(tagId);

        // Assert
        Assert.Equal(product.Tags.ToString(), expectedTags);
    }

    [Fact]
    public void Product_IsRetired_RemoveExistingTag_ThrowsProductLifecycleException()
    {
        // Arrange
        const string name = "name";
        const string description = "description";
        const int price = 142;
        var productCategory = new ProductCategory(1, "category name", "category description");
        var product = new Product(
            name,
            description,
            price,
            productCategory);
        var tagId = new TagId(1);
        product.AddTag(tagId);
        product.Retire();

        // Act/Assert
        Assert.Throws<ProductLifecycleException>(() => product.RemoveTag(tagId));
    }

    [Fact]
    public void Product_IsRetired_RemoveUnassociatedTag_ProductUnchanged()
    {
        // Arrange
        const string name = "name";
        const string description = "description";
        const int price = 142;
        var productCategory = new ProductCategory(1, "category name", "category description");
        var product = new Product(
            name,
            description,
            price,
            productCategory);
        var tagId = new TagId(1);
        var expectedTags = new string(product.Tags.ToString());

        // Act
        product.Retire();
        product.RemoveTag(tagId);

        // Assert
        Assert.Equal(product.Tags.ToString(), expectedTags);
    }

    [Fact]
    public void Product_IsRetired_CategoryAdded_ThrowsProductLifecycleException()
    {
        // Arrange
        const string name = "name";
        const string description = "description";
        const int price = 142;
        var oldCategory = new ProductCategory(1, "category name", "category description");
        var newCategory = new ProductCategory(2, "category name", "category description");
        var product = new Product(
            name,
            description,
            price,
            oldCategory);
        product.Retire();

        // Act/Assert
        Assert.Throws<ProductLifecycleException>(() => product.SetCategory(newCategory));
    }

    [Fact]
    public void Product_IsRetired_ExistingCategoryAdded_ProductUnchanged()
    {
        // Arrange
        const string name = "name";
        const string description = "description";
        const int price = 142;
        var productCategory = new ProductCategory(1, "category name", "category description");
        var product = new Product(
            name,
            description,
            price,
            productCategory);
        var expectedCategory = new string(product.Category.ToString());
        product.Retire();

        // Act
        product.SetCategory(productCategory);

        // Assert
        Assert.Equal(product.Category.ToString(), expectedCategory);
    }
}