using Microsoft.EntityFrameworkCore;
using Photos.Domain.DataBaseEntity;
using Photos.Domain.Repositories;
using Photos.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Photos.Application.Repositories
{
    public class EventRepository : DataBaseRepository<EventEntity, int>, IEventRepository
    {
        public EventRepository(PhotosDBContext photosDBContext) : base(photosDBContext)
        {

        }

        public async Task<IEnumerable<EventEntity>> GetAllEventsAsync(CancellationToken cancellationToken = default)
        {
            return await _photosDBContext.Events.ToListAsync();
        }
    }
}