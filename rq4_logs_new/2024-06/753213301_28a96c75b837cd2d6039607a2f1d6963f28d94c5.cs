using amazon_backend.Models;
using FluentValidation;
using MediatR;

namespace amazon_backend.CQRS.Commands.ReviewRequests
{
    public class LikeReviewCommandRequest : IRequest<Result<string>>
    {
        public string reviewId { get; set; }
    }
    public class LikeReviewValidator : AbstractValidator<LikeReviewCommandRequest>
    {
        public LikeReviewValidator()
        {
            RuleFor(x => x.reviewId)
                .Must(x => Guid.TryParse(x, out var result) == true)
                .WithMessage("Incorrect {PropertyName} format");
        }
    }
}