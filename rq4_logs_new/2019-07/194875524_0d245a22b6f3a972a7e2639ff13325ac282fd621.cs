using System.ComponentModel.DataAnnotations;

namespace RedSoft.Test.Api.Attributes
{
	public class DecimalGreaterThanZeroAttribute : ValidationAttribute
	{
		private readonly string fieldName;

		public DecimalGreaterThanZeroAttribute()
		{
			fieldName = "Field";
		}

		public DecimalGreaterThanZeroAttribute(string fieldName)
		{
			this.fieldName = fieldName;
		}


		public override bool IsValid(object value)
		{
			return value != null && decimal.TryParse(value.ToString(), out var dec) && dec > 0;
		}

		protected override ValidationResult IsValid(object value, ValidationContext validationContext)
		{
			if (!IsValid(value))
			{
				return new ValidationResult(GetErrorMessage());
			}

			return ValidationResult.Success;
		}
		
		private string GetErrorMessage()
		{
			return $"{fieldName} should be decimal and greater than zero";
		}
	}
}