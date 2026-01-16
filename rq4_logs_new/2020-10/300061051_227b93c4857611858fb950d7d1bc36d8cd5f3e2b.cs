using AutoFixture;
using AutoFixture.AutoNSubstitute;
using EightCare.Domain.KeeperAggregate;
using EightCare.Domain.KeeperAggregate.Abstractions;
using EightCare.Infrastructure.Repositories;
using FluentAssertions;
using NSubstitute;
using System.Linq;
using Xunit;

namespace EightCare.UnitTests.Repositories
{
    public class InMemoryKeeperRepositoryTests
    {
        private readonly IFixture _fixture;
        private readonly IKeeperInMemoryContext _keeperInMemoryContext;
        private readonly InMemoryKeeperRepository _inMemoryKeeperRepository;

        public InMemoryKeeperRepositoryTests()
        {
            _fixture = new Fixture();
            _fixture.Customize(new AutoNSubstituteCustomization());

            _keeperInMemoryContext = _fixture.Freeze<IKeeperInMemoryContext>();
            _inMemoryKeeperRepository = _fixture.Create<InMemoryKeeperRepository>();
        }

        [Fact]
        public void GetById_ExistingElement_ShouldReturnKeeper()
        {
            // Arrange
            var keepers = _fixture.CreateMany<Keeper>().ToList();
            var expectedKeeper = keepers.First();

            _keeperInMemoryContext.Keepers.Returns(keepers);

            // Act
            var foundKeeper = _inMemoryKeeperRepository.GetById(expectedKeeper.Id);

            // Assert
            foundKeeper.Should().NotBeNull()
                       .And
                       .Should().BeEquivalentTo(expectedKeeper, options => options.ComparingByMembers<Keeper>());
        }
    }
}