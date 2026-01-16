using FluentValidation;
using GradeBook.Models.Write;

namespace GradeBook.Models.Validation
{
    public class CreateGradeValidator : AbstractValidator<CreateGrade>
    {
        public CreateGradeValidator()
        {
            RuleFor(c => c.PupilId).NotEmpty();
            RuleFor(c => c.LessonId).NotEmpty();
            RuleFor(c => c.IsAbsent).NotNull();

            When(c => !(c.IsAbsent), () =>
            {
                RuleFor(c => c.Mark).NotEmpty().InclusiveBetween(1, 12).WithMessage("Mark is incorrect");
            });
            When(c => c.IsAbsent, () =>
            {
                RuleFor(c => c.Mark).Empty().WithMessage("Pupil cannot have \'Is absent\' sign and mark simultaneously");
            });
        }
    }
}