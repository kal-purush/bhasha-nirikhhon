using SchedulearnBackend.Models;

namespace SchedulearnBackend.Controllers.DTOs
{
    public class UserWithoutPassword
    {
        public UserWithoutPassword() 
        { 
        }

        public UserWithoutPassword(User user)
        {
            Id = user.Id;
            Name = user.Name;
            Surname = user.Surname;
            Email = user.Email;
            JobTitle = user.JobTitle;
            TeamId = user.TeamId;
            LimitId = user.LimitId;
        }

        public int Id { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
        public string Email { get; set; }
        public virtual JobTitle JobTitle { get; set; }
        public int? TeamId { get; set; }
        public int? LimitId { get; set; }
    }

    public class UserWithToken: UserWithoutPassword
    {
        public UserWithToken() 
        {
        }

        public UserWithToken(User user, string token): base(user) 
        {
            Token = token;
        }

        public string Token { get; set; }
    }
}