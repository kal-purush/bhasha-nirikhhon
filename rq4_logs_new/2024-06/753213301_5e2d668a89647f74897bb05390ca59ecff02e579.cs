using amazon_backend.Models;
using amazon_backend.Profiles.UserProfiles;
using FluentValidation;
using MediatR;

namespace amazon_backend.CQRS.Commands.UserRequests
{
    public class ChangeUserRoleCommandRequest:IRequest<Result<ClientProfile>>
    {
        public string userId { get; set;}
    }
    public class ChangeUserRoleValidator : AbstractValidator<ChangeUserRoleCommandRequest>
    {
        public ChangeUserRoleValidator()
        {
            RuleFor(x => x.userId)
                .Must(id => Guid.TryParse(id, out var result) == true)
                .WithMessage("Incorrect {PropertyName} format");
        }
    }
}