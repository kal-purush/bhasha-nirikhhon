namespace API.DTOs
{
    public class JobRegisterDto
    {
        public string Title { get; set; }
        public int ConfirmedOrgId { get; set; }
        public string LogoUrl { get; set; }
        public string Description { get; set; }
        public int Salary { get; set; }
        public string City { set; get; }
        public string ProvinceOrState { get; set; }
        public string Country { get; set; }
        public string Genres { get; set; }
        public string JobType { get; set; }
        public string SkillsRequired { get; set; }
        public string ApplicationUrl { get; set; }
        public DateTime Deadline { get; set; }

    }
}