using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using FluentAssertions;
using Newtonsoft.Json;
using SFA.DAS.FAT.Domain.Courses;
using SFA.DAS.FAT.Domain.Extensions;
using SFA.DAS.FAT.MockServer;
using SFA.DAS.FAT.Web.AcceptanceTests.Infrastructure;
using TechTalk.SpecFlow;

namespace SFA.DAS.FAT.Web.AcceptanceTests.Steps
{
    [Binding]
    public sealed class CourseProviderDetailSteps
    {
        private readonly ScenarioContext _context;

        public CourseProviderDetailSteps(ScenarioContext context)
        {
            _context = context;
        }

        [Then ("there are zero training providers")]
        public async Task ThenThePageContentIncludesTheFollowing()
        {
            var response = _context.Get<HttpResponseMessage>(ContextKeys.HttpResponse);

            var actualContent = await response.Content.ReadAsStringAsync();
            var json = DataFileManager.GetFile("course-provider-details-notfound.json");
            var expectedApiResponse = JsonConvert.DeserializeObject<TrainingCourseProviderDetails>(json);

            actualContent.Should().Be(HttpUtility.HtmlEncode(expectedApiResponse.CourseProviderDetails.DeliveryModes.Count() == 1));
        }

    }
}