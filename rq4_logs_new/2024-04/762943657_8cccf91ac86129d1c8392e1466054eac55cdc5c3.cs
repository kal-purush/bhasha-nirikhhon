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
    /// Counter Party Service test class
    /// </summary
    [TestFixture]
    public class CounterPartyServiceTests : UnitTestBase
    {
        /// <summary>
        /// Firm Service
        /// </summary>
        private ICounterPartyService service = null!;

        /// <summary>
        /// Repository
        /// </summary>
        private IRepository<CounterParty> repository = null!;

        /// <summary>
        /// SetUp
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            var mockRepositoryLogger =
                new Mock<ILogger<Repository<CounterParty>>>();

            var mockServiceLogger =
                new Mock<ILogger<CounterPartyService>>();

            this.context = DatabaseMock.Mock;

            this.repository = new Repository<CounterParty>(context,
                mockRepositoryLogger.Object);

            this.service = new CounterPartyService(repository,
                mockServiceLogger.Object);
        }

        /// <summary>
        /// Add async test methods
        /// </summary>
        /// <returns>(void)</returns>
        [Test]
        public async Task AddAsync_ShouldWorkProperly()
        {
            var invalidCounterParty = await this.repository.GetByIdAsync(1);

            var counterPartyBeforeAdding = this.repository.All().Count();

            await this.SeedData();

            var counterPartysAfterAdding = this.repository.All().Count();

            var addedCounterParty1 = await this.repository.GetByIdAsync(1);
            var addedCounterParty2 = await this.repository.GetByIdAsync(2);

            Assert.Multiple(() =>
            {
                Assert.That(counterPartyBeforeAdding,
                Is.EqualTo(counterPartysAfterAdding - 2));

                Assert.That(invalidCounterParty,
                    Is.EqualTo(null));

                Assert.That(addedCounterParty1?.Id,
                    Is.EqualTo(1));

                Assert.That(addedCounterParty2?.Id,
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

            var validCounterParty = await this.service.GetByIdAsync(1);

            Assert.Multiple(() =>
            {
                Assert.That(1,
                    Is.EqualTo(validCounterParty.Id));

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
                .GetAllByPredicateAsync("Name", "CounterParty1")
                .ToListAsync();

            var all2 = await this.service
                .GetAllByPredicateAsync("Name", "FalseCounterParty")
                .ToListAsync();

            var all3 = await this.service
                .GetAllByPredicateAsync("False False", "some counterParty name...")
                .ToListAsync();

            var all4 = await this.service
                .GetAllByPredicateAsync("Name", "CounterParty")
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

            await this.service.DeleteAsync(new CounterPartyServiceModel()
            {
                Id = 1
            });

            var allAfter = await this.service.GetAllAsync();
            var countAfter = allAfter.Count();

            var invalidModel = new CounterPartyServiceModel()
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
            var nameBefore = "CounterParty1";
            var bankCodeBefore = "BankCode1";
            var bankIBANBefore = "BankIBAN1";
            var vadtliBefore = "VATLI1";
            var VATNumberBefore = "VATNumber1";

            await this.SeedData();

            var newModel = new CounterPartyServiceModel()
            {
                Id = 1,
                Name = "new",
                FirmId = 1,
                BankId = 1,
                BankCode = "new",
                BankIBAN = "new",
                ValueAddedTaxLawId = "new",
                VATNumber = "new"
            };

            await this.service.UpdateAsync(newModel);

            var updated = await this.service.GetByIdAsync(1);

            var invalidModel = new CounterPartyServiceModel()
            {
                Id = 1000,
                Name = "fake"
            };

            Assert.Multiple(() =>
            {
                Assert.That(nameBefore != newModel.Name);
                Assert.That(bankCodeBefore != newModel.BankCode);
                Assert.That(bankIBANBefore != newModel.BankIBAN);
                Assert.That(vadtliBefore != newModel.ValueAddedTaxLawId);
                Assert.That(VATNumberBefore != newModel.VATNumber);
                Assert.That(updated.Name == newModel.Name);
                Assert.That(updated.BankCode == newModel.BankCode);
                Assert.That(updated.BankIBAN == newModel.BankIBAN);
                Assert.That(updated.ValueAddedTaxLawId == newModel.ValueAddedTaxLawId);
                Assert.That(updated.VATNumber == newModel.VATNumber);
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
            var counterParty1 = new CounterParty()
            {
                Id = 1,
                Name = "CounterParty1",
                BankCode = "BankCode1",
                BankIBAN = "BankIBAN1",
                BankId = 1,
                FirmId = 1,
                ValueAddedTaxLawId = "VATLI1",
                VATNumber = "VATNumber1"
            };
            var counterParty2 = new CounterParty()
            {
                Id = 2,
                Name = "CounterParty2",
                BankCode = "BankCode2",
                BankIBAN = "BankIBAN2",
                BankId = 1,
                FirmId = 1,
                ValueAddedTaxLawId = "VATLI2",
                VATNumber = "VATNumber2"
            };

            var address1 = new Address()
            {
                Id = 1,
                StreetName = "TestStreet1",
                StreetNumber = "TestNumber1",
                TownName = "TestTown1"
            };
            var address2 = new Address()
            {
                Id = 2,
                StreetName = "TestStreet2",
                StreetNumber = "TestNumber2",
                TownName = "TestTown2"
            };

            var firm1 = new Firm()
            {
                Id = 1,
                Name = "Firm1",
                AddressId = 1
            };
            var firm2 = new Firm()
            {
                Id = 2,
                Name = "Firm2",
                AddressId = 2
            };

            var bank1 = new Bank()
            {
                Id = 1,
                Name = "Bank1",
                AddressId = 1
            };
            var bank2 = new Bank()
            {
                Id = 2,
                Name = "Bank2",
                AddressId = 2
            };

            await this.context.Addresses.AddAsync(address1);
            await this.context.Addresses.AddAsync(address2);
            await this.context.Firms.AddAsync(firm1);
            await this.context.Firms.AddAsync(firm2);
            await this.context.Banks.AddAsync(bank1);
            await this.context.Banks.AddAsync(bank2);
            await this.context.SaveChangesAsync();

            await this.repository.AddAsync(counterParty1);
            await this.repository.AddAsync(counterParty2);
        }
    }
}