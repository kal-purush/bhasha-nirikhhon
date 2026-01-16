using System.ComponentModel.DataAnnotations;

namespace comp1640_dotnet.DTOs.Requests
{
	public class ChangePasswordRequest
	{
		[Required]
		[DataType(DataType.EmailAddress)]
		public string Email { get; set; } = string.Empty ;
		[Required]
		[DataType(DataType.Password)]
		public string Password { get; set; } = string.Empty;
		[Required]
		[DataType(DataType.Password)]
		public string NewPassword { get; set; } = string.Empty;
		[Required]
		[DataType(DataType.Password)]
		public string ConfirmPassword { get; set; } = string.Empty;
	}
}
 