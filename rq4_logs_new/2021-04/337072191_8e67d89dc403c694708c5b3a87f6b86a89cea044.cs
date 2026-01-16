using Common.DTOs.TeamDTO;
using FluentValidation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace API.Validators.TeamValidators
{
    public class TeamUpdateValidator : AbstractValidator<TeamUpdateDto>
    {
        public TeamUpdateValidator()
        {
            RuleFor(x => x.Name)
                .NotEmpty().WithMessage("{PropertyName} cannot be empty")
                .MaximumLength(50).WithMessage("{PropertyName} is too long");
        }
    }
}