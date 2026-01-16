using AutoFixture;
using EightCare.Domain.KeeperAggregate;
using EightCare.Infrastructure.Repositories;
using FluentAssertions;
using System.Linq;
using Xunit;

namespace EightCare.UnitTests.Repositories
{
    public class InMemoryKeeperContextTests
    {
        private readonly IFixture _fixture;
        private readonly InMemoryKeeperContext _keeperContext;

        public InMemoryKeeperContextTests()
        {
            _fixture = new Fixture();
            _keeperContext = new InMemoryKeeperContext();
        }

        [Fact]
        public void SaveChanges_ShouldSetIdForExistingKeepers()
        {
            // Arrange
            var keepers = _fixture.CreateMany<Keeper>();

            _keeperContext.Keepers.AddRange(keepers);

            // Act
            _keeperContext.SaveChanges();

            // Assert
            _keeperContext.Keepers.Select(x => x.Id).Should().OnlyHaveUniqueItems();
        }
    }
}