using amazon_backend.Models;
using amazon_backend.Profiles.ProductProfiles;
using FluentValidation;
using MediatR;

namespace amazon_backend.CQRS.Commands.WishListRequests
{
    public class AddItemToWishCommandRequest:IRequest<Result<ProductCardProfile>>
    {
        public string productId { get; set; }
    }
    public class AddItemToWishValidator : AbstractValidator<AddItemToWishCommandRequest>
    {
        public AddItemToWishValidator()
        {
            RuleFor(x => x.productId)
               .Must(x => Guid.TryParse(x, out var result) == true)
               .WithMessage("Incorrect {PropertyName} format");
        }
    }
}