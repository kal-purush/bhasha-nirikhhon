using System.ComponentModel.DataAnnotations;

namespace HogwartsRunningClub.Models
{
    public class UserRace
    {

        [Key]
        public int UserRaceId { get; set; }

        [Required]
        public string UserId { get; set; }

        [Required]
        public int RaceId { get; set; }

        [Required]
        public string RaceBib { get; set; }

        public string RaceBibImage { get; set; }


        public Race Race { get; set; }
        public ApplicationUser User { get; set; }

    }
}