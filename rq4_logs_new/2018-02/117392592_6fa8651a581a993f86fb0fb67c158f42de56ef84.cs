using CivicdAPI.Models;
using CivicdAPI.Models.Profile;
using System;
using System.Linq;
using System.Web.Http;

namespace CivicdAPI.Controllers
{

    [Authorize]
    [RoutePrefix("api/")]
    public class ProfileController
    {
        private ApplicationDbContext context = new ApplicationDbContext();

        public ProfileController()
        {
        }

        [Route("user/{UserEmail}")]
        public UserViewModel GetUserByEmail(string UserEmail)
        {
            var matchedUser = context.Users.FirstOrDefault(u => u.Email.Equals(UserEmail));
            return new UserViewModel
            {
                Email = matchedUser.Email,
                DisplayName = matchedUser.DisplayName,
                FirstName = matchedUser.FirstName,
                LastName = matchedUser.LastName,
                ProfileDescription = matchedUser.ProfileDescription,
                StreetAddressOne = matchedUser.Address.StreetAddressOne,
                StreetAddressTwo = matchedUser.Address.StreetAddressTwo,
                City = matchedUser.Address.City,
                State = matchedUser.Address.State,
                ZipCode = matchedUser.Address.ZipCode
            };
        }

        [Route("")]
        public OrganizationViewModel GetOrganizationById(int organizationId)
        {
            //TODO: Context doesn't contain an organizations entity collection. Talk to Ele about that 
            return null;
        }
    }

}