using System;
using ReptileTracker.Feeding.Model;
using ReptileTracker.Infrastructure.Persistence;

namespace ReptileTracker.Feeding.Service;

public class FeedingService(IGenericRepository<FeedingEvent> feedingRepository) : IFeedingService
{
    public FeedingEvent AddFeedingEvent(FeedingEvent feedingEvent)
    {
        try
        {
            feedingRepository.Add(feedingEvent);
            feedingRepository.Save();
            return feedingEvent;
        }
        catch (Exception ex)
        {
            throw ex;
        }
    }
}