using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using MediatR;
using Microsoft.EntityFrameworkCore;
using ScheduleDevelopmentKit.Common.Exceptions;
using ScheduleDevelopmentKit.DataAccess;

namespace ScheduleDevelopmentKit.Modules.Core.Rooms.Queries
{
    public static class GetRoomsByCampusId
    {
        [PublicAPI]
        public record Query(Guid CampusId) : IRequest<Response>;

        [PublicAPI]
        public record Response(IReadOnlyCollection<Response.Room> Rooms)
        {
            public record Room(Guid Id, string Number, bool BelongsToFaculty);
        };

        [UsedImplicitly]
        public class QueryHandler : IRequestHandler<Query, Response>
        {
            private readonly SdkDbContext _dbContext;

            public QueryHandler(SdkDbContext dbContext)
            {
                _dbContext = dbContext;
            }

            public async Task<Response> Handle(Query request, CancellationToken cancellationToken)
            {
                var campus = await _dbContext.Campuses
                                       .Include(c => c.Rooms)
                                       .FirstOrDefaultAsync(c => c.Id == request.CampusId, cancellationToken);

                if (campus is null)
                    throw new EntityNotFoundException($"Campus with id {request.CampusId}");

                var rooms = campus.Rooms
                                        .Select(r => new Response.Room(r.Id, r.Number.Value, r.BelongsToFaculty))
                                        .ToList();

                return new Response(rooms);
            }
        }
    }
}