namespace MarketVault.Infrastructure.Data.Models
{
    using Microsoft.AspNetCore.Identity;
    using System.ComponentModel.DataAnnotations;
    using static MarketVault.Infrastructure.Constants.DataConstants.UserConstants;

    /// <summary>
    /// Application user - custom user class
    /// </summary>
    public class ApplicationUser : IdentityUser
    {
        /// <summary>
        /// User first name
        /// </summary>
        [Required]
        [MaxLength(FirstNameMaxLength)]
        public string FirstName { get; set; } = string.Empty;

        /// <summary>
        /// User last name
        /// </summary>
        [Required]
        [MaxLength(LastNameMaxLength)]
        public string LastName { get; set; } = string.Empty;
    }
}