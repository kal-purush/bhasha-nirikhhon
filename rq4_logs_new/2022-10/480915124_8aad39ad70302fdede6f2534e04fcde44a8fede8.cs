using System.ComponentModel.DataAnnotations;

namespace SocialClubApp.ViewModels
{
    public class ClubMemberViewModel
    {
        //To avoid NullReferenceExceptions at runtime,
        //initialise Claims and Roles with a new list in the constructor.
        public ClubMemberViewModel()
        {
            Claims = new List<string>();
        }

        public string Id { get; set; }
        [Required]
        public string UserName { get; set; }
        [Required]
        [EmailAddress]
        public string Email { get; set; }
        public string? ProfileImageUrl { get; set; }

        //[DataType(DataType.Date)]
        //[DisplayFormat(DataFormatString = "{0:yyyy-MM-dd}", ApplyFormatInEditMode = true)]
        public DateTime? Joined { get; set; }
        public string? City { get; set; }
        public string? State { get; set; }
        public List<string> Claims { get; set; }
    }
}