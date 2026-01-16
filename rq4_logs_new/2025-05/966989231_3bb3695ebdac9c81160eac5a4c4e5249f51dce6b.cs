using Bazario.AspNetCore.Shared.Results;
using Bazario.Identity.Application.Features.Auth.DTO;
using MediatR;

namespace Bazario.Identity.Application.Features.Auth.Commands.RegisterAdmin
{
    public sealed record RegisterAdminCommand(
        string Email,
        string FirstName,
        string LastName,
        DateOnly BirthDate,
        string PhoneNumber) : IRegisterApplicationUserBaseCommand, IRequest<Result>;
}