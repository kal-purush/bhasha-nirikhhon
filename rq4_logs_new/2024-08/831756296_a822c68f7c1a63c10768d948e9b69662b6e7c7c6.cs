using API.Middlewares.Exceptions;
using Application.ViewModels;
using FluentValidation;

namespace API.Validation
{
    public class ContactValidator : AbstractValidator<ContactRequestModel>
    {
        public ContactValidator()
        {
            RuleFor(x => x.Name)
                .NotEmpty().WithMessage("Campo nome obrigatorio.")
                .Length(2, 50).WithMessage("Nome deve ter entre 2 e 50 caracteres.");

            RuleFor(x => x.Email)
                .NotEmpty().WithMessage("Campo email obrigatorio.")
                .EmailAddress().WithMessage("Formato de email inválido.");

            RuleFor(x => x.Ddd)
                .NotEmpty().WithMessage("Campo Ddd obrigatorio.")
                .Matches(@"^\d{2}$").WithMessage("Ddd deve conter 2 numeros.");

            RuleFor(x => x.Phone)
               .NotEmpty().WithMessage("Campo phone é obrigatório.")
               .Length(1, 10).WithMessage("Campo phone nao pode ter mais de 10 caracteres.")
               .Matches(@"^[\d-]+$").WithMessage("Phone deve conter somente digitos numericos e/ou o caracter '-'");
        }

        public void IsValid(ContactRequestModel contact)
        {
            var res = Validate(contact);

            if (!res.IsValid)
            {
                var fields = new List<Field>();
                foreach (var error in res.Errors)
                {
                    Field field = new()
                    {
                        Message = error.ErrorMessage,
                        Name = error.PropertyName,
                        Value = error.AttemptedValue.ToString()
                    };
                    fields.Add(field);
                }

                DataValidationException.Throw("002", "Dados invalidos", "Dados de contato invalido", fields);
            }
        }
    }
}

