using FluentValidation;
using JetBrains.Annotations;
using MAVN.Service.SmartVouchers.Client.Models.Requests;

namespace MAVN.Service.SmartVouchers.Validation
{
    [UsedImplicitly]
    public class CampaignImageFileRequestValidator : AbstractValidator<CampaignImageFileRequest>
    {
        public CampaignImageFileRequestValidator()
        {
            RuleFor(x => x.Id)
                .NotEmpty()
                .WithMessage(x => $"{nameof(x.Id)} required");

            RuleFor(x => x.CampaignId)
                .Must(x => x != default)
                .WithMessage(x => $"{nameof(x.CampaignId)} required");

            RuleFor(x => x.Name)
                .NotEmpty()
                .WithMessage(x => $"{nameof(x.Name)} required");

            RuleFor(x => x.Type)
                .NotEmpty()
                .WithMessage(x => $"{nameof(x.Type)} required");

            RuleFor(x => x.Content)
                .NotNull()
                .WithMessage(x => $"{nameof(x.Content)} required")
                .Must(x => x.Length > 0)
                .WithMessage(x => $"{nameof(x.Content)} can't be an empty array");
        }
    }
}