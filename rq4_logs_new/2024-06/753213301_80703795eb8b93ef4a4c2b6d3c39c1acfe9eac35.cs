using amazon_backend.Models;
using FluentValidation;
using MediatR;

namespace amazon_backend.CQRS.Commands.OrderRequests
{
    public class CancelOrderCommandRequest:IRequest<Result<string>>
    {
        public string OrderId { get; set;}
    }
    public class CancelOrderValidator : AbstractValidator<CancelOrderCommandRequest>
    {
        public CancelOrderValidator()
        {
            RuleFor(x => x.OrderId)
              .Must(x => Guid.TryParse(x, out var result) == true)
              .WithMessage("Incorrect {PropertyName} format");
        }
    }
}