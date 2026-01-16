using Database.Models;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WebApp.ViewModels
{
    public class UserAssignmentListViewModel
    {
        
        public IEnumerable<IFormFile> Files { get; set; }
       
        public IEnumerable<UserAssignment> UserAssignments { get; set; }
        public IEnumerable<User> Users { get; set; }
    }
}