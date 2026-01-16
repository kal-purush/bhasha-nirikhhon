using System;
using System.Threading;
using System.Threading.Tasks;
using FluentValidation;
using JetBrains.Annotations;
using MediatR;
using Microsoft.EntityFrameworkCore;
using ScheduleDevelopmentKit.Common.Exceptions;
using ScheduleDevelopmentKit.DataAccess;
using ScheduleDevelopmentKit.Domain.Entities;
using ScheduleDevelopmentKit.Domain.ValueObjects;

namespace ScheduleDevelopmentKit.Modules.Core.Rooms.Commands
{
    public static class EditRoom
    {
        [PublicAPI]
        public record Command(Guid Id, string Number, bool BelongsToFaculty) : IRequest<Response>;

        [UsedImplicitly]
        public class CommandValidator : AbstractValidator<Command>
        {
            public CommandValidator()
            {
                RuleFor(c => c.Number).Length(1,10);
                RuleFor(c => c.Number).Matches(@"^[0-9]{0,9}[0-9a-zA-Z]{0,1}$");
            }
        }

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
                    throw new EntityNotFoundException($"Room with Id {request.Id} not found");

                room.Number = new RoomNumber(request.Number);
                room.BelongsToFaculty = request.BelongsToFaculty;

                _dbContext.Update(room);
                await _dbContext.SaveChangesAsync(cancellationToken);

                return new Response(room.Id, room.Number.Value);
            }
        }
    }
}