using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace MyBlazorApp.Server.Entities
{
    [Table("UserVacationsBudget")]
    public class UserVacationBudget
    {
        [Key]
        public int Id { get; set; }

        [Required]
        public int UserId { get; set; }
        public virtual User User { get; set; }

        [Required]
        public short Year { get; set; }

        [Required]
        public int TotalDays { get;  set; }

    }
}