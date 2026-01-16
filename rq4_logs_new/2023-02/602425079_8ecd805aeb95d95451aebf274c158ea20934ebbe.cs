using System;
using System.Collections.Generic;

namespace StudentAttandanceLibrary.Models
{
    public partial class Account
    {
        public string AccountId { get; set; } = null!;
        public string Email { get; set; } = null!;
        public string Password { get; set; } = null!;
        public int? RoleId { get; set; }
        public bool? Status { get; set; }

        public virtual Role? Role { get; set; }
        public virtual Admin? Admin { get; set; }
        public virtual Student? Student { get; set; }
        public virtual Teacher? Teacher { get; set; }
    }
}