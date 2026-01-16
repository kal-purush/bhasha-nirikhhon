using System;
using System.Threading;
using System.Threading.Tasks;
using FluentValidation;
using JetBrains.Annotations;
using MediatR;
using ScheduleDevelopmentKit.Common.Exceptions;
using ScheduleDevelopmentKit.DataAccess;
using ScheduleDevelopmentKit.Domain.Entities;
using ScheduleDevelopmentKit.Domain.ValueObjects;

namespace ScheduleDevelopmentKit.Modules.Core.Campuses.Commands
{
    public static class EditCampus
    {
        [PublicAPI]
        public record Command(
            Guid Id,
            string Name,
            string Address) : IRequest<Response>;

        [UsedImplicitly]
        public class CommandValidator : AbstractValidator<Command>
        {
            public CommandValidator()
            {
                RuleFor(c => c.Name).MaximumLength(100).NotEmpty();
                RuleFor(c => c.Address).MaximumLength(300).NotEmpty();
            }
        }

        [PublicAPI]
        public record Response(Guid Id, string Name);

        [UsedImplicitly]
        public class CommandHandler : IRequestHandler<Command, Response>
        {
            private readonly SdkDbContext _sdkDbContext;

            public CommandHandler(SdkDbContext context)
            {
                _sdkDbContext = context;
            }

            public async Task<Response> Handle(Command request, CancellationToken cancellationToken)
            {
                var campus = await _sdkDbContext.Campuses.FindAsync(new object[]{ request.Id }, cancellationToken);

                if (campus is null)
                    throw new EntityNotFoundException($"Campus with id {request.Id} not found");

                campus.Address = new CampusAddress(request.Address);
                campus.Name = new CampusName(request.Name);

                _sdkDbContext.Campuses.Update(campus);
                await _sdkDbContext.SaveChangesAsync(cancellationToken);

                return new Response(campus.Id, campus.Name.Value);
            }
        }
    }
}