using System.Collections.Generic;
using System.Linq;
using AutoFixture.NUnit3;
using FluentAssertions;
using NUnit.Framework;
using SFA.DAS.FAT.Web.Models;

namespace SFA.DAS.FAT.Web.UnitTests.Models.CourseProvidersViewModelTests
{
    public class WhenDeterminingIfThereAreLocations
    {
        [Test, AutoData]
        public void Then_If_There_Are_Delivery_Modes_HasLocations_Is_True(CourseProvidersViewModel model)
        {
            model.HasLocations.Should().BeTrue();
        }
        
        [Test, AutoData]
        public void Then_If_There_Are_Not_Delivery_Modes_HasLocations_Is_False(CourseProvidersViewModel model)
        {
            model.Providers = model.Providers.Select(c =>
            {
                c.DeliveryModes = new List<DeliveryModeViewModel>();
                return c;
            }).ToList();

            model.HasLocations.Should().BeFalse();
        }
    }
}