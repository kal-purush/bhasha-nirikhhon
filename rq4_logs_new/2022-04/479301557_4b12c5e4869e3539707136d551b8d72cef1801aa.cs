using LinkBakery.Core.Models;
using LinkBakery.Core.Repositories.Interfaces;
using LinkBakery.Web.Redirect.Services.Interfaces;

namespace LinkBakery.Web.Redirect.Services
{
    public class TrackingLinkService : ITrackingLinkService
    {
        private readonly ITrackingLinkRepository _trackingLinkRepository;


        public TrackingLinkService(ITrackingLinkRepository trackingLinkRepository)
        {
            _trackingLinkRepository = trackingLinkRepository;
        }


        public IEnumerable<TrackingLink> GetAll()
        {
            return _trackingLinkRepository.GetAll();
        }
    }
}