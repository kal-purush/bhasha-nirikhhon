using SchedulearnBackend.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SchedulearnBackend.Controllers.DTOs
{
    public class CreateNewUser
    {
        public string Name { get; set; }
        public string Surname { get; set; }
        public string Email { get; set; }
        public int JobTitleId { get; set; }
        public int ManagingUserId { get; set; }

        public User CreateUser() { 
            return new User()
            {
                Name = Name,
                Surname = Surname,
                Email = Email,
                JobTitleId = JobTitleId,
            };
        }
    }
}