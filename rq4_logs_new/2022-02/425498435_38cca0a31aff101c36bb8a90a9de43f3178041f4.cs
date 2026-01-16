using System;
using System.Threading;
using System.Threading.Tasks;
using FluentValidation;
using JetBrains.Annotations;
using MediatR;
using ScheduleDevelopmentKit.Common.Exceptions;
using ScheduleDevelopmentKit.DataAccess;

namespace ScheduleDevelopmentKit.Modules.Core.Rooms.Commands
{
    public static class DeleteRoom
    {
        [PublicAPI]
        public record Command(Guid Id) : IRequest<Response>;

        [PublicAPI]
        public record Response(Guid Id, string Number);

        [UsedImplicitly]
        public class CommandHandler : IRequestHandler<Command, Response>
        {
            private readonly SdkDbContext _dbContext;

            public CommandHandler(SdkDbContext dbContext)
            {
                _dbContext = dbContext;
            }

            public async Task<Response> Handle(Command request, CancellationToken cancellationToken)
            {
                var room = await _dbContext.Rooms.FindAsync(new object[] { request.Id }, cancellationToken);

                if (room is null)
                    throw new EntityNotFoundException($"Room with id {request.Id} not found");

                _dbContext.Remove(room);
                await _dbContext.SaveChangesAsync(cancellationToken);

                return new Response(room.Id, room.Number.Value);
            }
        }
    }
}