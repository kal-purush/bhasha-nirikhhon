namespace jobsearch.Core.Models.EF
{
    public class CompanyAgentEntity : BaseModel<int>
    {
        public string? FirstName { get; set; }

        public string? LastName { get; set; }

        public string Email { get; set; }

        public string? AvatarUrl { get; set; }

        public string? Position { get; set; }

        public bool IsActive { get; set; }

        public string PasswordHash { get; set; }

        public string? PublicEmail { get; set; }

        public string? PublicPhone { get; set; }

        #region FK Company
        public int CompanyId { get; set; }

        public CompanyEntity Company { get; set; }
        #endregion
    }
}