using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace DataAccessLayer
{
    [Table("UserRoles")]
    public class UserRole
    {
        public UserRole()
        {
            //this.UserId = null;
            this.IsEnabled = false;
            DateCreated = DateTime.Now;
        }
        [Required]
        public DateTime DateCreated { get; set; }

        [Required]
        public bool IsEnabled { get; set; }

        [Key]
        [Column(Order =1)]
        public int UserId { get; set; }

        [Key]
        [Column(Order = 2)]
        public Enums.RoleEnum RoleId { get; set; }

        public User User { get; set; }
        public Role Role { get; set; }

    }
}