using System;
using System.Collections.Generic;
using System.Text;

namespace Data_.Dtos
{
    public class UserWithAchievementsDTO
    {
        public Guid Id { get; set; }
        public string Email { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }
        public int Xp { get; set; }
        public Guid? AvatarId { get; set; }
        public ICollection<string> Roles { get; set; }
        public ICollection<Achievement> Achievements { get; set; }
    }
}