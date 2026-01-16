using amazon_backend.Models;
using amazon_backend.Profiles.AboutProductProfiles;
using amazon_backend.Profiles.ProductProfiles;
using amazon_backend.Profiles.ProductPropertyProfiles;
using FluentValidation;
using MediatR;

namespace amazon_backend.CQRS.Commands.ProductCommands
{
    public class CreateProductCommandRequest : IRequest<Result<ProductViewProfile>>
    {
        public string name { get; set; }
        public string? code { get; set; }
        public int categoryId { get; set; }
        public double price { get; set; }
        public int? discount { get; set; }
        public int quantity { get; set; }
        public List<string> images { get; set; }
        public List<ProductPropFormProfile> productDetails { get; set; }
        public List<AboutProductFormProfile>? aboutProduct { get; set; }
    }
    public class CreateProductValidator : AbstractValidator<CreateProductCommandRequest>
    {
        private readonly int imagesMaxCount = 10;
        private readonly int propTextMaxCharCount = 30;
        private readonly int aboutProductTextMaxCharCount = 250;

        public CreateProductValidator()
        {
            // name validate
            RuleFor(x => x.name)
                .NotEmpty()
                .WithMessage("Product name cannot be empty");

            // code validate
            RuleFor(x => x.code).Must((code) =>
            {
                foreach (var item in code)
                {
                    if (!int.TryParse(item.ToString(), out var _))
                    {
                        return false;
                    }
                }
                return true;
            }).WithMessage("Barcode is invalid")
            .Length(13)
            .WithMessage("Barcode length must be 13 characters")
            .When(x => x.code != null);

            // category validate
            RuleFor(x => x.categoryId)
                .NotEmpty()
                .WithMessage("Please select category")
                .GreaterThan(0);

            // price validate
            RuleFor(x => x.price).NotEmpty()
                .WithMessage("Price cannot be empty")
                .GreaterThan(0)
                .WithMessage("Price must be a positive number");

            // discount validate
            RuleFor(x => x.discount).GreaterThan(0).LessThan(101).WithMessage("Discount must be a positive number. Only numbers from 0 to 100 are allowed")
                .When(x=>x.discount.HasValue);

            // quantity validate
            RuleFor(x => x.quantity)
                .GreaterThan(0)
                .WithMessage("Quantity must be a positive number")
                .NotEmpty()
                .WithMessage("Quantity cannot be empty");

            // images validate
            RuleFor(x => x.images)
                .Must(x=>x.Count <= imagesMaxCount)
                .WithMessage($"There should be a maximum of {imagesMaxCount} photos")
                .Must(images => images.Count >= 1)
                .WithMessage("Add at least one photo");

            RuleForEach(x => x.images).ChildRules(images =>
            {
                images.RuleFor(x => x)
                .Must(id => Guid.TryParse(id, out var result) == true)
                .WithMessage("Incorrect {PropertyName} format"); ;
            });

            // productDetails validate
            RuleFor(x => x.productDetails)
                .Must(productDetails => productDetails.Count >= 1)
                .WithMessage("You must describe at least 1 option");

            RuleForEach(x => x.productDetails).ChildRules(productDetails =>
            {
                productDetails.RuleFor(x => x.name)
                .NotEmpty()
                .WithMessage("Name cannot be empty");

                productDetails.RuleFor(x => x.text)
                .NotEmpty()
                .WithMessage("Text cannot be empty")
                .MaximumLength(propTextMaxCharCount)
                .WithMessage($"Text should be no more than {propTextMaxCharCount} characters");
            });

            // aboutProduct validate

            RuleForEach(x => x.aboutProduct).ChildRules(aboutProduct =>
            {
                aboutProduct.RuleFor(x => x.name).NotEmpty()
                .WithMessage("Name cannot be empty");

                aboutProduct.RuleFor(x => x.text).NotEmpty()
                .WithMessage("Text cannot be empty")
                .MaximumLength(aboutProductTextMaxCharCount)
                .WithMessage($"Text should be no more than {aboutProductTextMaxCharCount} characters");
            }).When(x => x.aboutProduct != null);
        }
    }
}