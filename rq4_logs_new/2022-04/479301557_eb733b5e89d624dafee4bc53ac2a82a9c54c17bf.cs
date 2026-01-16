using LinkBakery.Application.Contracts.Persistence;
using LinkBakery.Domain.Entities;
using Moq;
using System.Collections.Generic;

namespace LinkBakery.Application.UnitTests.Mocks
{
    public class RepositoryMocks
    {
        public static Mock<ITrackingLinkRepository> GetTrackingLinkRepository()
        {
            var trackingLinks = new List<TrackingLink>
            {
                new TrackingLink
                {
                    Id = 1,
                    Key = "gh",
                    TargetUrl = "https://www.github.com",
                    IsActive = true,
                    RedirectWithQueryParameter = true,
                    CreatedDate = System.DateTime.Now,
                    LastModifiedDate = null
                }
            };


            var mockTrackingLinkRepository = new Mock<ITrackingLinkRepository>();

            mockTrackingLinkRepository.Setup(repo => repo.GetAllAsync()).ReturnsAsync(trackingLinks);

            mockTrackingLinkRepository.Setup(repo => repo.AddAsync(It.IsAny<TrackingLink>())).ReturnsAsync(
                (TrackingLink trackingLink) =>
                {
                    trackingLinks.Add(trackingLink);
                    return trackingLink;
                }
            );


            return mockTrackingLinkRepository;
        }
    }
}