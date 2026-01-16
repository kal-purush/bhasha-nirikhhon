using FluentValidation;
using GradeBook.DataAccess.Entities;
using GradeBook.Models.Write;

namespace GradeBook.Models.Validation
{
    public class UpdateRoleValidator : AbstractValidator<UpdateRole>
    {
        public UpdateRoleValidator()
        {
            RuleFor(c => c.Id).NotEmpty();
            RuleFor(c => c.Role).NotNull();

            When(c => c.Role == Role.Pupil, () =>
            {
                RuleFor(c => c.ClassId).NotEmpty();
            });
            When(c => !(c.Role == Role.Pupil), () =>
            {
                RuleFor(c => c.ClassId).Null();
            });
        }
    }
}