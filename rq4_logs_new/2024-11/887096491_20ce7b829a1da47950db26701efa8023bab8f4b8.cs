using FluentValidation;
using WolfDen.Domain.Entity;

namespace WolfDen.Application.Validators.LeaveManagement
{
    public class LeaveRequestValidator : AbstractValidator<LeaveRequest>
    {
        public LeaveRequestValidator()
        {
            RuleFor(x => x.HalfDay).NotEmpty().WithMessage("Choose Half Day or full day");
            RuleFor(x => x.FromDate).NotEmpty().WithMessage("From Date Required");
            RuleFor(x => x.ToDate).NotEmpty().WithMessage("To Date Required");
            RuleFor(x => x.Description).NotEmpty().WithMessage("Description Required");
        }
    }
}