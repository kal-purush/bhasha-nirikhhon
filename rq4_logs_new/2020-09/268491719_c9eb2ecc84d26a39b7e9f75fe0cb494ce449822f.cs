using AutoFixture.NUnit3;
using FluentAssertions;
using NUnit.Framework;
using SFA.DAS.FAT.Domain.Courses;
using SFA.DAS.FAT.Web.Models;

namespace SFA.DAS.FAT.Web.UnitTests.Models.CourseProvidersViewModelTests
{
    public class WhenBuildingSortLink
    {
        [Test, AutoData]
        public void And_SortOrder_Distance_Then_Link_To_Name(CourseProvidersViewModel model)
        {
            model.SortOrder = ProviderSortBy.Distance;

            var link = model.BuildSortLink();

            link.Should().Be($"?locations={model.Location}&sortorder={ProviderSortBy.Name}");
        }

        [Test, AutoData]
        public void And_SortOrder_Name_Then_Link_To_Distance(CourseProvidersViewModel model)
        {
            model.SortOrder = ProviderSortBy.Name;

            var link = model.BuildSortLink();

            link.Should().Be($"?locations={model.Location}&sortorder={ProviderSortBy.Distance}");
        }
    }
}