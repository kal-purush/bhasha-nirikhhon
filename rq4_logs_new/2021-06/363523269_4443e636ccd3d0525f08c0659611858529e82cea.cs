using HelpI.API.Security.Domain.Models;

namespace HelpI.API.Security.Domain.Services.Communication
{
    public class AuthenticationResponse
    {
        public int Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Email { get; set; }
        public string Token { get; set; }

        public AuthenticationResponse(Player user, string token )
        {
            Id = user.Id;
            FirstName = user.PersonalProfile.FirstName;
            LastName = user.PersonalProfile.LastName;
            Email = user.Email;
            Token = token;
        }

      
    }
}