using System.Net;
using AutoFixture.NUnit3;
using FluentAssertions;
using Moq;
using NUnit.Framework;
using SFA.DAS.FindApprenticeship.Jobs.Application.Services;
using SFA.DAS.FindApprenticeship.Jobs.Domain.Interfaces;
using SFA.DAS.FindApprenticeship.Jobs.Infrastructure.Api.Requests;
using SFA.DAS.FindApprenticeship.Jobs.Infrastructure.Api.Responses;
using SFA.DAS.Testing.AutoFixture;

namespace SFA.DAS.FindApprenticeship.Jobs.UnitTests.Services.FindApprenticeshipJobsServiceTests;

public class WhenGetSavedSearches
{
    [Test, MoqAutoData]
    public async Task Then_The_Api_Is_Called_And_Results_Returned(
        int pageNumber,
        int pageSize,
        string lastRunDateTime,
        int maxApprenticeshipSearchResultCount,
        string sortOrder,
        GetCandidateSavedSearchesApiResponse apiResponse,
        [Frozen] Mock<IOuterApiClient> outerApiClient,
        FindApprenticeshipJobsService service)
    {
        var apiRequest = new GetSavedSearchesApiRequest(pageNumber, pageSize, lastRunDateTime,maxApprenticeshipSearchResultCount);
        outerApiClient
            .Setup(x => x.Get<GetCandidateSavedSearchesApiResponse>(
                It.Is<GetSavedSearchesApiRequest>(c => 
                    c.GetUrl.Equals(apiRequest.GetUrl)
                )
            )).ReturnsAsync(new ApiResponse<GetCandidateSavedSearchesApiResponse>(apiResponse, HttpStatusCode.OK, ""));
        
        var actual = await service.GetSavedSearches(pageNumber, pageSize, lastRunDateTime, maxApprenticeshipSearchResultCount, sortOrder);

        actual.Should().BeEquivalentTo(apiResponse);
    }
}