using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using MediatR;
using ScheduleDevelopmentKit.Common.Exceptions;
using ScheduleDevelopmentKit.DataAccess;

namespace ScheduleDevelopmentKit.Modules.Core.Campuses.Commands
{
    public static class DeleteCampus
    {
        [PublicAPI]
        public record Command(Guid Id) : IRequest<Response>;

        [PublicAPI]
        public record Response(Guid Id, string Name);

        [UsedImplicitly]
        public class CommandHandler : IRequestHandler<Command, Response>
        {
            private SdkDbContext _dbContext;

            public CommandHandler(SdkDbContext dbContext)
            {
                _dbContext = dbContext;
            }

            public async Task<Response> Handle(Command request, CancellationToken cancellationToken)
            {
                var campus = await _dbContext.Campuses.FindAsync(new object[] { request.Id }, cancellationToken);

                if (campus is null)
                    throw new EntityNotFoundException($"Campus with id {request.Id} not found");

                _dbContext.Remove(campus);
                await _dbContext.SaveChangesAsync(cancellationToken);

                return new Response(campus.Id, campus.Name.Value);
            }
        }
    }
}