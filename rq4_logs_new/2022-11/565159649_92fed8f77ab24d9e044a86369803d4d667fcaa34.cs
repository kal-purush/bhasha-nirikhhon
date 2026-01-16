using System;

using CleanArchMvc.Domain.Entities;
using CleanArchMvc.Domain.Validation;

using Xunit;
using FluentAssertions;


namespace CleanArchMvc.Domain.Tests
{
    public class ProductUnitTests
    {
        [Fact]
        public void CreateProduct_Should_Successfuly_Create_A_Product_When_Using_Valid_Params()
        {
            Action action = () =>
            {
                _ = new Product("Product", "My product", 10m, 1, null);
            };

            action
                .Should()
                .NotThrow<DomainExceptionValidation>();
        }

        [Fact]
        public void CreateProduct__Should_Throw_DomainExceptionValidation_When_Pass_An_Id_Less_Than_1()
        {
            Action action = () =>
            {
                _ = new Product(0, "Product", "My product", 10m, 1, null);
            };

            action
                .Should()
                .Throw<DomainExceptionValidation>()
                .WithMessage("Invalid id value");
        }

        [Theory]
        [InlineData("")]
        [InlineData("   ")]
        [InlineData(null)]
        public void CreateProduct__Should_Throw_DomainExceptionValidation_When_Pass_An_Empty_Name(string name)
        {
            Action action = () =>
            {
                _ = new Product(1, name, "My product", 10m, 1, null);
            };

            action
                .Should()
                .Throw<DomainExceptionValidation>()
                .WithMessage("Invalid name. Name is required");
        }

        [Fact]
        public void CreateProduct__Should_Throw_DomainExceptionValidation_When_Pass_A_Short_Name()
        {
            Action action = () =>
            {
                _ = new Product(1, "Pr", "My product", 10m, 1, null);
            };

            action
                .Should()
                .Throw<DomainExceptionValidation>()
                .WithMessage("Invalid name. too short, minimum 3 characters");
        }

        [Theory]
        [InlineData("")]
        [InlineData("   ")]
        [InlineData(null)]
        public void CreateProduct__Should_Throw_DomainExceptionValidation_When_Pass_An_Empty_Description(string description)
        {
            Action action = () =>
            {
                _ = new Product(1, "Product", description, 10m, 1, null);
            };

            action
                .Should()
                .Throw<DomainExceptionValidation>()
                .WithMessage("Invalid description. Description is required");
        }

        [Fact]
        public void CreateProduct__Should_Throw_DomainExceptionValidation_When_Pass_A_Short_Description()
        {
            Action action = () =>
            {
                _ = new Product(1, "Pssdfr", "Pro", 10m, 1, null);
            };

            action
                .Should()
                .Throw<DomainExceptionValidation>()
                .WithMessage("Invalid description. minimum 5 characters");
        }

        [Fact]
        public void CreateProduct__Should_Throw_DomainExceptionValidation_When_Pass_A_Price_Less_Than_Zero()
        {
            Action action = () =>
            {
                _ = new Product(1, "Pssdfr", "sdsdfsdfsdfs", -0.1m, 1, null);
            };

            action
                .Should()
                .Throw<DomainExceptionValidation>()
                .WithMessage("Invalid price value");
        }

        [Fact]
        public void CreateProduct__Should_Throw_DomainExceptionValidation_When_Pass_A_Stock_Less_Than_Zero()
        {
            Action action = () =>
            {
                _ = new Product(1, "Pssdfr", "sdsdfsdfsdfs", 1, -1, null);
            };

            action
                .Should()
                .Throw<DomainExceptionValidation>()
                .WithMessage("Invalid stock value");
        }

        [Theory]
        [InlineData("    ")]
        [InlineData("")]
        [InlineData(null)]
        public void CreateProduct__Should_Successfully_Create_A_Product_With_An_Empty_Image(string image)
        {
            Action action = () =>
            {
                _ = new Product(1, "Pssdfr", "sdsdfsdfsdfs", 1, 1, image);
            };

            action
                .Should()
                .NotThrow();
        }

        [Fact]
        public void CreateProduct__Should_Throw_DomainExceptionValidation_When_Pass_A_Image_With_More_Than_250_Chars()
        {
            Action action = () =>
            {
                _ = new Product(1, "Pssdfr", "sdsdfsdfsdfs", 1, 1, new string('a', 251));
            };

            action
                .Should()
                .Throw<DomainExceptionValidation>()
                .WithMessage("Invalid image name. too long, maximum 250 characters");
        }

        [Fact]
        public void UpdateProduct__Should_Throw_DomainExceptionValidation_When_Pass_A_Category_Id_Less_Then_1()
        {
            Action action = () =>
            {
                var product = new Product(1, "aaa", "aaaaa", 0, 0, new string('a', 250));

                product.Update("aaa", "aaaaa", 0, 0, new string('a', 250), 0);
            };

            action
                .Should()
                .Throw<DomainExceptionValidation>()
                .WithMessage("Invalid category id");
        }
    }
}