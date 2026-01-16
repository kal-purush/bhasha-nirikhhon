using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentValidation;

namespace WolfDen.Application.Requests.Queries.Attendence.AttendanceSummary
{
    public class AttendanceSummaryQueryValidator : AbstractValidator<AttendanceSummaryQuery>
    {
        public AttendanceSummaryQueryValidator()
        {
            RuleFor(x => x.Year).NotEmpty().GreaterThan(0).WithMessage("Year must be greater than 0");
            RuleFor(x => x.Month).NotEmpty().GreaterThanOrEqualTo(1).LessThanOrEqualTo(12).WithMessage("Month must be between 1 to 12 ");
        }
    }
}