using Microsoft.AspNetCore.Identity;

namespace WolfDen.Domain.Entity
{
    public class User : IdentityUser
    {
        public new string Id
        {
            get => base.Id;
            private set => base.Id = value;
        }
        public new string Email
        {
            get => base.Email;
            private set => base.Email = value;
        }


        public User()
        {

        }
        public User(string email,string Name)
        {
            Email = email;
            UserName = Name;
        }
    }
}