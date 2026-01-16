using System.ComponentModel.DataAnnotations;
using ImpulsionaTech.Contas.Domain.Shared.Utils;

namespace ImpulsionaTech.Contas.Domain.Shared.Annotation
{
  public class ValidaCPFAtributte : ValidationAttribute
  {
    public ValidaCPFAtributte() : base("CPF informado não é valido")
    {

    }

    protected override ValidationResult IsValid(object value, ValidationContext validationContext)
    {
      bool valido = Util.ValidaCPF(cpf: value.ToString());
      if (valido)
        return null;
      return new ValidationResult(base.FormatErrorMessage(validationContext.MemberName)
          , new string[] { validationContext.MemberName });
    }
  }
}