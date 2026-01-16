using FluentValidation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WolfDen.Application.Requests.Queries.Employees.GetEmployeeIdSignUp;

namespace WolfDen.Application.Requests.Queries.Employees.GetEmployeeIDSignUp
{
    public class GetEmployeeIdSignUpQueryValidator:AbstractValidator<GetEmployeeIDSignUpQuery>
    {
        public GetEmployeeIdSignUpQueryValidator()
        {
            RuleFor(x=>x.EmployeeCode).NotEmpty().WithMessage("Employee Code cannot be NULL");
            RuleFor(x => x.RFId).NotEmpty().WithMessage("RFId cannot be NULL");

        }
    }
}