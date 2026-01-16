using amazon_backend.Models;
using amazon_backend.Profiles.ProductProfiles;
using FluentValidation;
using MediatR;

namespace amazon_backend.CQRS.Commands.ReviewImageRequests
{
    public class CreateProductImageCommandRequest : IRequest<Result<ProductImageProfile>>
    {
        public IFormFile productImage { get; set; }
    }

    public class CreateProductImageValidator : AbstractValidator<CreateProductImageCommandRequest>
    {
        public CreateProductImageValidator()
        {
            RuleFor(x => x.productImage.Length).LessThanOrEqualTo(5 * 1024 * 1024)
                    .WithMessage("File size must be less than or equal to 5 MB");
            RuleFor(x => x.productImage.ContentType).Must(contentType =>
                contentType == "image/jpeg" || contentType == "image/png")
                .WithMessage("Only JPEG and PNG files are allowed");
        }
    }

}