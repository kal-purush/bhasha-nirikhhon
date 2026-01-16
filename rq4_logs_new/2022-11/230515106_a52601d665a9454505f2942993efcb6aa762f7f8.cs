using BeerStuff.Api.BeerGrain;
using BeerStuff.Api.Mappings;
using FluentAssertions;
using Xunit;

namespace BeerStuff.Api.Tests.Mappings
{
    public class CreateBeerGrainRequestMapTests
    {
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void Map_Success(bool nullableFieldsOmitted)
        {
            // arrange
            var apiRequest = new CreateBeerGrainRequest
            {
                Name = "Grain Name",
            };

            if (!nullableFieldsOmitted)
            {
                apiRequest.Manufacturer = "GrainCorp";
                apiRequest.Lovibond = 50;
                apiRequest.PotentialGravity = new decimal(1.1);
            }

            // act
            var result = CreateBeerGrainRequestMap.Map(apiRequest);

            // assert
            result.Should().NotBeNull();
            result.Name.Should().Be(apiRequest.Name);
            result.Manufacturer.Should().Be(apiRequest.Manufacturer);
            result.Lovibond.Should().Be(apiRequest.Lovibond);

            if (nullableFieldsOmitted)
            {
                result.PotentialGravity.Should().BeNull();
            }
            else
            {
                var potentialGravity = (decimal)result.PotentialGravity;
                potentialGravity.Should().Be(new decimal(1.1));
            }
        }
    }
}