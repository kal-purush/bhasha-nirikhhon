using Sample.Application.Commands;
using Sample.Application.Commands.Handlers;
using Sample.Domain.Interfaces.Commands;
using Sample.Domain.Models;

namespace Sample.Api.Configuration.CQRS.Commands
{
    public static class UserInjection
    {
        public static IServiceCollection AddUsersDependency(this IServiceCollection services)
        {
            services.AddScoped(typeof(ICommandHandler<CreateUserCommand, Response>), typeof(CreateUserCommandHandler));
            return services;
        }
    }
}