using WebApi.Domain.Common;

namespace WebApi.Domain.Users
{
    public class Employee : Entity<string>
    {
        public Employee(User identityUser)
        {
            RegistrationConfirmed = false;
            IdentityUser = identityUser;
            Id = identityUser.Id;
        }

        private Employee()
        {
        }

        public User IdentityUser { get; private set; }

        public bool RegistrationConfirmed { get; private set; }

        public void ConfirmRegistration() => RegistrationConfirmed = true;
    }
}