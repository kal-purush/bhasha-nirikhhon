using System;
using System.ComponentModel.DataAnnotations;

namespace HelpI.API.Security.Application.Transform.Resources
{
    public class SaveExpertProfileResource
    {
        [Required]
        public String Elo { get; set; }
        [Required]
        public String GameUserName { get; set; }
        [Required]
        public String ExperienceStory {get; set; }
        [Required]
        public String WhyMe { get; set; }

        [Required] [DataType(DataType.Date)]
        public DateTime StartedPlaying { get; set; }
    }
}