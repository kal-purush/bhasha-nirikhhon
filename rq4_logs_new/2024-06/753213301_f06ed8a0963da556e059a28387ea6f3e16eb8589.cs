using amazon_backend.Models;
using amazon_backend.Profiles.CartItemProfiles;
using FluentValidation;
using MediatR;

namespace amazon_backend.CQRS.Commands.CartRequests
{
    public class AddNewItemToCartCommandRequest : IRequest<Result<CartItemProfile>>
    {
        public string productId { get; set; }
        public int quantity { get; set; } = 1;
    }
    public class AddNewItemToCartValidator : AbstractValidator<AddNewItemToCartCommandRequest>
    {
        public AddNewItemToCartValidator()
        {
            RuleFor(x => x.productId)
             .Must(x => Guid.TryParse(x, out var result) == true)
             .WithMessage("Incorrect {PropertyName} format");

            RuleFor(x => x.quantity).GreaterThan(0).WithMessage("Quantity must be a positive value");
        }
    }
}