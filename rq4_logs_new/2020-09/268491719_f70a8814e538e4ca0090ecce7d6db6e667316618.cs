using System;
using System.Collections.Generic;
using System.Linq;
using AutoFixture.NUnit3;
using FluentAssertions;
using NUnit.Framework;
using SFA.DAS.FAT.Application.Courses.Queries.GetCourseProviders;
using SFA.DAS.FAT.Domain.Extensions;
using SFA.DAS.FAT.Web.Models;

namespace SFA.DAS.FAT.Web.UnitTests.Models.CourseProvidersViewModelTests
{
    public class WhenConstructingModel
    {
        [Test, AutoData]
        public void Then_Sets_Properties(GetCourseProvidersRequest request, GetCourseProvidersResult result)
        {
            var model = new CourseProvidersViewModel(request, result);

            model.Course.Should().BeEquivalentTo((CourseViewModel)result.Course);
            model.Providers.Should().BeEquivalentTo(
                result.Providers.Select(provider => (ProviderViewModel)provider));
            model.Total.Should().Be(result.Total);
            model.TotalFiltered.Should().Be(result.TotalFiltered);
            model.Location.Should().Be(result.Location);
            model.SortOrder.Should().Be(request.SortOrder);
        }

        [Test, AutoData]
        public void Then_Builds_Delivery_Modes(GetCourseProvidersRequest request, GetCourseProvidersResult result)
        {
            var expectedDeliveryModes = new List<DeliveryModeOptionViewModel>();
            foreach (DeliveryModeType deliveryModeType in Enum.GetValues(typeof(DeliveryModeType)))
            {
                expectedDeliveryModes.Add(new DeliveryModeOptionViewModel
                {
                    DeliveryModeType = deliveryModeType,
                    Description = deliveryModeType.GetDescription(),
                    Selected = request.DeliveryModes.Any(type => type == deliveryModeType)
                });
            }

            var model = new CourseProvidersViewModel(request, result);

            model.DeliveryModes.Should().BeEquivalentTo(expectedDeliveryModes);
        }
    }
}