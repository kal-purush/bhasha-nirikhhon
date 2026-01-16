using FluentValidation;
using Microsoft.EntityFrameworkCore;
using WolfDen.Infrastructure.Data;

namespace WolfDen.Application.Requests.Commands.LeaveManagement.LeaveTypes.AddLeaveType
{
    public class AddLeaveTypeValidator : AbstractValidator<AddLeaveTypeCommand>
    {
        private readonly WolfDenContext _context;

        public AddLeaveTypeValidator(WolfDenContext context)
        {
            _context = context;

            RuleFor(x => x.TypeName).NotEmpty().WithMessage("Type Name Required")
                .MustAsync(BeUniqueTypeName).WithMessage("Type Name Already Exist");
        }

        private async Task<bool> BeUniqueTypeName(string typeName, CancellationToken cancellationToken)
        {
            return !await _context.LeaveType
                .AnyAsync(x => x.TypeName == typeName, cancellationToken);
        }

    }
}