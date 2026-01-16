using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TechTitans.Models
{
    [Table("UserDemographicsDetails")]
    public class UserDemographicsDetails
    {
        [Key]
        [Column("user_id")]
        public int UserId { get; set; }

        [Column("name")]
        public string Name { get; set; }

        [Column("gender")]
        public string Gender { get; set; }

        [Column("date_of_birth")]
        public DateTime DateOfBirth { get; set; }

        [Column("country")]
        public string Country { get; set; }

        [Column("language")]
        public string Language { get; set; }

        [Column("race")]
        public string Race { get; set; }

        [Column("premium_user")]
        public bool PremiumUser { get; set; }
    }
}