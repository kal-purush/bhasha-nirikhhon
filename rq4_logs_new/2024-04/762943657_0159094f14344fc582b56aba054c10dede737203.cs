namespace MarketVault.Tests.UnitTests.ServiceTests
{
    using MarketVault.Core.Contracts;
    using MarketVault.Core.Exceptions;
    using MarketVault.Core.Implementations;
    using MarketVault.Core.Models;
    using MarketVault.Core.Services.Impementations;
    using MarketVault.Core.Services.Interfaces;
    using MarketVault.Infrastructure.Data.Models;
    using MarketVault.Tests.UnitTests.Mocks;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.Extensions.Logging;
    using Moq;
    using NUnit.Framework;

    /// <summary>
    /// Item Group Service tests class
    /// </summary>
    [TestFixture]
    public class ItemGroupServiceTests : UnitTestBase
    {
        /// <summary>
        /// Item Group Service
        /// </summary>
        private IItemGroupService service = null!;

        /// <summary>
        /// Repository
        /// </summary>
        private IRepository<ItemGroup> repository = null!;

        /// <summary>
        /// Product Service
        /// </summary>
        private IProductService productService = null!;

        /// <summary>
        /// SetUp method
        /// </summary>
        [SetUp]
        public void Setup()
        {
            var mockRepositoryLogger =
                new Mock<ILogger<Repository<ItemGroup>>>();

            var mockServiceLogger =
                new Mock<ILogger<ItemGroupService>>();

            this.context = DatabaseMock.Mock;


            this.repository = new Repository<ItemGroup>(context,
                mockRepositoryLogger.Object);

            this.productService = new Mock<IProductService>()
                .Object;

            this.service = new ItemGroupService(repository,
                productService,
                mockServiceLogger.Object);
        }

        /// <summary>
        /// Add async test methods
        /// </summary>
        /// <returns>(void)</returns>
        [Test]
        public async Task AddAsync_ShouldWorkProperly()
        {
            var invalidItemGroup = await this.repository.GetByIdAsync(1);

            var itemGroupBeforeAdding = this.repository.All().Count();

            await this.SeedData();

            var itemGroupsAfterAdding = this.repository.All().Count();

            var addedItemGroup1 = await this.repository.GetByIdAsync(1);
            var addedItemGroup2 = await this.repository.GetByIdAsync(2);

            Assert.Multiple(() =>
            {
                Assert.That(itemGroupBeforeAdding,
                Is.EqualTo(itemGroupsAfterAdding - 2));

                Assert.That(invalidItemGroup,
                    Is.EqualTo(null));

                Assert.That(addedItemGroup1?.Id,
                    Is.EqualTo(1));

                Assert.That(addedItemGroup2?.Id,
                    Is.EqualTo(2));
            });
        }

        /// <summary>
        /// Get by id async test methods
        /// </summary>
        /// <returns>(void)</returns>
        [Test]
        public async Task GetByIdAsync_ShouldWorkProperly()
        {
            await this.SeedData();

            var validItemGroup = await this.service.GetByIdAsync(1);

            Assert.Multiple(() =>
            {
                Assert.That(1,
                    Is.EqualTo(validItemGroup.Id));

                Assert.ThrowsAsync<EntityNotFoundException>(() =>
                {
                    return this.service.GetByIdAsync(-1);
                });
            });
        }

        /// <summary>
        /// Get all async test methods
        /// </summary>
        /// <returns>(void)</returns>
        [Test]
        public async Task GetAllAsync_ShouldWorkProperly()
        {
            await this.SeedData();

            var all = await this.service.GetAllAsync();

            Assert.Multiple(() =>
            {
                Assert.That(all.Count() == 2);
                Assert.That(all.Where(a => a.Id == 1).Count() == 1);
                Assert.That(all.Where(a => a.Id == 2).Count() == 1);
            });
        }

        /// <summary>
        /// Get count async test methods
        /// </summary>
        /// <returns>(void)</returns>
        [Test]
        public async Task GetCountAsync_ShouldWorkProperly()
        {
            Assert.That(await
                this.service.GetCountAsync(),
                Is.EqualTo(0));

            await this.SeedData();

            Assert.That(await
                this.service.GetCountAsync(),
                Is.EqualTo(2));
        }

        /// <summary>
        /// Get all by predicate async test methods
        /// </summary>
        /// <returns>(void)</returns>
        [Test]
        public async Task GetAllByPredicateAsync_ShouldWorkProperly()
        {
            await this.SeedData();

            var all1 = await this.service
                .GetAllByPredicateAsync("Name", "ItemGroup1")
                .ToListAsync();

            var all2 = await this.service
                .GetAllByPredicateAsync("Name", "FalseItemGroup")
                .ToListAsync();

            var all3 = await this.service
                .GetAllByPredicateAsync("False False", "some itemGroup name...")
                .ToListAsync();

            var all4 = await this.service
                .GetAllByPredicateAsync("Name", "ItemGroup")
                .ToListAsync();

            Assert.Multiple(() =>
            {
                Assert.That(all1.Count == 1);
                Assert.That(all2.Count == 0);
                Assert.That(all3.Count == 0);
                Assert.That(all4.Count == 2);
            });
        }

        /// <summary>
        /// Delete async test methods
        /// </summary>
        /// <returns>(void)</returns>
        [Test]
        public async Task DeleteAsync_ShouldWorkProperly()
        {
            await this.SeedData();

            var allBefore = await this.service.GetAllAsync();
            var countBefore = allBefore.Count();

            await this.service.DeleteAsync(new ItemGroupServiceModel()
            {
                Id = 1
            });

            var allAfter = await this.service.GetAllAsync();
            var countAfter = allAfter.Count();

            var invalidModel = new ItemGroupServiceModel()
            {
                Id = 1000,
                Name = "FAKE"
            };

            Assert.Multiple(() =>
            {
                Assert.That(countBefore, Is.EqualTo(2));
                Assert.That(countAfter, Is.EqualTo(1));
                Assert.That(allBefore.Count(a => a.Id == 1) == 1);
                Assert.That(allAfter.Any(a => a.Id == 1) == false);
                Assert.ThrowsAsync<EntityNotFoundException>(() =>
                {
                    return this.service.DeleteAsync(invalidModel);
                });
            });
        }

        /// <summary>
        /// Update async test methods
        /// </summary>
        /// <returns>(void)</returns>
        [Test]
        public async Task UpdateAsync_ShouldWorkProperly()
        {
            var nameBefore = "ItemGroup1";

            await this.SeedData();

            var newModel = new ItemGroupServiceModel()
            {
                Id = 1,
                Name = "new"
            };

            await this.service.UpdateAsync(newModel);

            var updated = await this.service.GetByIdAsync(1);

            var invalidModel = new ItemGroupServiceModel()
            {
                Id = 1000,
                Name = "fake"
            };

            Assert.Multiple(() =>
            {
                Assert.That(nameBefore != newModel.Name);
                Assert.That(updated.Name == newModel.Name);
                Assert.ThrowsAsync<EntityNotFoundException>(() =>
                {
                    return this.service.UpdateAsync(invalidModel);
                });
            });
        }

        /// <summary>
        /// Private method for seeding data
        /// </summary>
        /// <returns>(void)</returns>
        private async Task SeedData()
        {
            var itemGroup1 = new ItemGroup()
            {
                Id = 1,
                Name = "ItemGroup1"
            };
            var itemGroup2 = new ItemGroup()
            {
                Id = 2,
                Name = "ItemGroup2"
            };

            await this.repository.AddAsync(itemGroup1);
            await this.repository.AddAsync(itemGroup2);
        }
    }
}