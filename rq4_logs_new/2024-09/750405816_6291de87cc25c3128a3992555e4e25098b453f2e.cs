using FluentAssertions;
using NUnit.Framework;
using SFA.DAS.EmployerRequestApprenticeTraining.Infrastructure.Api.Responses;
using SFA.DAS.EmployerRequestApprenticeTraining.Web.Models.EmployerRequest;
using System.Collections.Generic;

namespace SFA.DAS.EmployerRequestApprenticeTraining.Web.UnitTests.Models
{
    public class LocationsViewModelTests
    {
        private class TestLocationsViewModel : ILocationsViewModel
        {
            public string SameLocation { get; set; }
            public string SingleLocation { get; set; }
            public List<Region> Regions { get; set; } = new List<Region>();
        }

        [Test]
        public void GetMultipleLocations_ShouldReturnEmptyList_WhenNoRegionsExist()
        {
            // Arrange
            ILocationsViewModel viewModel = new TestLocationsViewModel
            {
                Regions = new List<Region>()
            };

            // Act
            var result = viewModel.GetMultipleLocations();

            // Assert
            result.Should().BeEmpty();
        }

        [Test]
        public void GetMultipleLocations_ShouldReturnSubregionNames_WhenRegionsExist()
        {
            // Arrange
            ILocationsViewModel viewModel = new TestLocationsViewModel
            {
                Regions = new List<Region>
                {
                    new Region { SubregionName = "Subregion1" },
                    new Region { SubregionName = "Subregion2" }
                }
            };

            // Act
            var result = viewModel.GetMultipleLocations();

            // Assert
            result.Should().BeEquivalentTo(new List<string> { "Subregion1", "Subregion2" });
        }
    }
}