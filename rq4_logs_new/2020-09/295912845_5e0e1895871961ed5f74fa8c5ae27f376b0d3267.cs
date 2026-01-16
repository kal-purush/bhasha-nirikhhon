using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using Moq;
using NUnit.Framework;
using OffiRent.API.Domain.Models;
using OffiRent.API.Domain.Repositories;
using OffiRent.API.Domain.Services.Communications;
using OffiRent.API.Services;

namespace OffiRent.API.Test
{
    public class CategoryServiceTest
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public async Task GetAllAsyncWhenNoCategoriesReturnsEmptyCollection()
        {
            // Arrange
            var mockCategoryRepository = GetDefaultICategoryRepositoryInstance();
            mockCategoryRepository.Setup(r => r.ListAsync())
                .ReturnsAsync(new List<Country>());
            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();
            var service = new CountryService(
                mockCategoryRepository.Object,
                mockUnitOfWork.Object);

            // Act
            List<Account> categories = (List<Account>) await service.ListAsync();
            var categoriesCount = categories.Count;

            // Assert
            categoriesCount.Should().Equals(0);
        }

        [Test]
        public async Task GetByIdAsyncWhenInvalidIdReturnsCategoryNotFoundReResponse()
        {
            // Arrange
            var mockCategoryRepository = GetDefaultICategoryRepositoryInstance();
            var categoryId = 1;
            mockCategoryRepository.Setup(r => r.FindById(categoryId))
                .Returns(Task.FromResult<Account>(null));
            var mockUnitOfWork = GetDefaultIUnitOfWorkInstance();
            var service = new CountryService(
                mockCategoryRepository.Object,
                mockUnitOfWork.Object);


            // Act
            CategoryResponse response = await service.GetByIdAsync(categoryId);
            var message = response.Message;

            // Assert
            message.Should().Be("Category not found");
        }

        private Mock<ICategoryRepository> GetDefaultICategoryRepositoryInstance()
        {
            return new Mock<ICategoryRepository>();
        }

        private Mock<IUnitOfWork> GetDefaultIUnitOfWorkInstance()
        {
            return new Mock<IUnitOfWork>();
        }

    }
}