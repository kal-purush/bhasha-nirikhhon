namespace MarketVault.Areas.Admin.AdminModels
{
    /// <summary>
    /// View Model for user entity
    /// </summary>
    public class UserViewModel
    {
        /// <summary>
        /// Model id
        /// </summary>
        public string Id { get; set; } = string.Empty;

        /// <summary>
        /// Model first name
        /// </summary>
        public string FirstName { get; set; } = string.Empty;

        /// <summary>
        /// Model last name
        /// </summary>
        public string LastName { get; set; } = string.Empty;

        /// <summary>
        /// Model email
        /// </summary>
        public string Email { get; set; } = string.Empty;

        /// <summary>
        /// Model role
        /// </summary>
        public string Role { get; set; } = string.Empty;
    }
}