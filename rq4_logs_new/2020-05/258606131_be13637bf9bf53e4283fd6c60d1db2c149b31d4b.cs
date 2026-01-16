using Microsoft.AspNetCore.Components;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.Extensions.DependencyInjection;
using NossoMercadoLivreAPI.Domain.Entities;
using NossoMercadoLivreAPI.Domain.Interfaces.Context;
using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;

namespace NossoMercadoLivreAPI.Domain.Validators
{
    public class UniqueUserValidator : ValidationAttribute
    {
        private IDbContext _context;

        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            _context = validationContext.GetService<IDbContext>();

            bool result = _context.Instance.Set<User>().Where(u => u.Email == value.ToString()).Any();

            return result ? new ValidationResult(FormatErrorMessage(validationContext.DisplayName)) : ValidationResult.Success;
        }
    }
}