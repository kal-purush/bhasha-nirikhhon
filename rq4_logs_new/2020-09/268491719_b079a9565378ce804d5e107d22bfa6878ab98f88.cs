using System.Linq;
using AutoFixture.NUnit3;
using FluentAssertions;
using NUnit.Framework;
using SFA.DAS.FAT.Web.Models;

namespace SFA.DAS.FAT.Web.UnitTests.Models.CourseProvidersViewModelTests
{
    public class WhenBuildingDeliveryModeLinks
    {
        [Test, AutoData]
        public void And_DeliveryMode_Selected_Then_Link_Returned(CourseProvidersViewModel model)
        {
            foreach (var deliveryMode in model.DeliveryModes)
            {
                deliveryMode.Selected = true;
            }

            var links = model.BuildClearDeliveryModeLinks();

            foreach (var deliveryMode in model.DeliveryModes.Where(viewModel => viewModel.Selected))
            {
                var link = links.Single(pair => pair.Key == deliveryMode.Description);
                var otherSelected = model.DeliveryModes
                    .Where(viewModel =>
                        viewModel.Selected &&
                        viewModel.DeliveryModeType != deliveryMode.DeliveryModeType)
                    .Select(viewModel => viewModel.DeliveryModeType);

                link.Value.Should().Be($"?location={model.Location}&deliveryModes={string.Join("&deliveryModes=", otherSelected)}&sortorder={model.SortOrder}");
            }
        }

        [Test, AutoData]
        public void And_DeliveryMode_Not_Selected_Then_Empty_List_Returned(CourseProvidersViewModel model)
        {
            foreach (var deliveryMode in model.DeliveryModes)
            {
                deliveryMode.Selected = false;
            }

            var links = model.BuildClearDeliveryModeLinks();

            links.Should().BeEmpty();
        }
    }
}