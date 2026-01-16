using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace StudentManager.Models.Auth
{
    public class PostAssignRole
    {
        public int UserId { get; set; }
        public int RoleId { get; set; }
    }
}