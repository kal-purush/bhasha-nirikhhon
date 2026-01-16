using AutoFixture;
using EightCare.Domain;
using FluentAssertions;
using Xunit;

namespace EightCare.UnitTests
{
    public class AnimalTests
    {
        private readonly IFixture _fixture;

        public AnimalTests()
        {
            _fixture = new Fixture();
        }

        [Fact]
        public void New_ShouldCreateAnimal()
        {
            // Arrange
            var commonName = _fixture.Create<string>();
            var scientificName = _fixture.Create<string>();

            // Act
            var animal = new Animal(commonName, scientificName);

            // Assert
            animal.Should().NotBeNull();
            animal.CommonName.Should().Be(commonName);
            animal.ScientificName.Should().Be(scientificName);
        }
    }
}