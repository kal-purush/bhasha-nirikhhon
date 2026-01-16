using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using ReptileTracker.EntityFramework;
using ReptileTracker.Feeding.Model;

namespace ReptileTracker.Infrastructure.Persistence;

public class FeedingRepository(ReptileContext context) : GenericRepository<FeedingEvent>(context), IFeedingRepository
{
    public async Task<List<FeedingEvent>> GetAllForReptile(int reptileId)
    {
        return await _context.FeedingEvents.Where(x => x.ReptileId == reptileId).ToListAsync();
    }
}