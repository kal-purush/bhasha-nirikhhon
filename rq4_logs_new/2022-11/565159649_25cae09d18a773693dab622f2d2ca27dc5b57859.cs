using CleanArchMvc.Domain.Entities;
using CleanArchMvc.Domain.Validation;

using Xunit;
using FluentAssertions;

using System;


namespace CleanArchMvc.Domain.Tests
{
    public class CategoryUnitTests
    {
        [Fact]
        public void CreateCategory__Should_Successfuly_Create_A_Category_When_Using_Valid_Parameters()
        {
            Action action = () =>
            {
                _ = new Category(1, "Category Name");
            };

            action
                .Should()
            .NotThrow<DomainExceptionValidation>();
        }

        [Fact]
        public void CreateCategory__Should_Throw_DomainExceptionValidation_When_Pass_Id_Less_Than_1()
        {
            Action action = () =>
            {
                _ = new Category(0, "Category Name");
            };

            var exception = action
                .Should()
                .Throw<DomainExceptionValidation>()
                .WithMessage("Invalid id value");
        }

        [Theory]
        [InlineData("")]
        [InlineData("    ")]
        [InlineData(null)]
        public void CreateCategory__Should_Throw_DomainExceptionValidation_When_Pass_An_Empty_Name(string name)
        {
            Action action = () =>
            {
                _ = new Category(0, name);
            };

            var exception = action
                .Should()
                .Throw<DomainExceptionValidation>()
                .WithMessage("Invalid name. Name is required");
        }

        [Fact]
        public void CreateCategory__Should_Throw_DomainExceptionValidation_When_Pass_A_Short_Name()
        {
            Action action = () =>
            {
                _ = new Category(0, "Ca");
            };

            var exception = action
                .Should()
                .Throw<DomainExceptionValidation>()
                .WithMessage("Invalid name. too short, mimnimum 3 characters");
        }
    }
}