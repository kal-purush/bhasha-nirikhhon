using amazon_backend.CQRS.Commands.ReviewImageRequests;
using amazon_backend.Data.Entity;
using amazon_backend.Models;
using amazon_backend.Profiles.CategoryProfiles;
using FluentValidation;
using MediatR;

namespace amazon_backend.CQRS.Commands.CategoryImageRequst
{
    public class CreateCategoryImageCommandRequst : IRequest<Result<List<CategoryImageProfile>>>
    {
        public int CategoryId { get; set; }
        public IFormFile categoryImages { get; set; }
        
    }
    public class CreateCategoryImageValidator : AbstractValidator<CreateCategoryImageCommandRequst>
    {
        public CreateCategoryImageValidator()
        {
            

            RuleFor(x => x.categoryImages).ChildRules(categoryImages =>
            {
                categoryImages.RuleFor(categoryImages => categoryImages.Length).LessThanOrEqualTo(5 * 1024 * 1024)
                    .WithMessage("File size must be less than or equal to 5 MB");

                categoryImages.RuleFor(categoryImages => categoryImages.ContentType).Must(contentType =>
                 contentType == "image/jpeg" || contentType == "image/png")
                 .WithMessage("Only JPEG and PNG files are allowed");
            });
        }
    }
}