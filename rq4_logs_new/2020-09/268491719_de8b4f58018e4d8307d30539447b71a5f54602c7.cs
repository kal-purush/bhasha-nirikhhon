using AutoFixture.NUnit3;
using FluentAssertions;
using NUnit.Framework;
using SFA.DAS.FAT.Web.Models;

namespace SFA.DAS.FAT.Web.UnitTests.Models.CourseProvidersViewModelTests
{
    public class WhenGettingHasLocations
    {
        [Test, AutoData]
        public void And_Location_Null_Then_False(
            CourseProvidersViewModel model)
        {
            model.Location = null;

            model.HasLocations.Should().BeFalse();
        }

        [Test, AutoData]
        public void And_Location_Whitespace_Then_False(
            CourseProvidersViewModel model)
        {
            model.Location = " ";

            model.HasLocations.Should().BeFalse();
        }

        [Test, AutoData]
        public void And_Location_Not_Null_Then_True(
            CourseProvidersViewModel model)
        {
            model.HasLocations.Should().BeTrue();
        }
    }
}