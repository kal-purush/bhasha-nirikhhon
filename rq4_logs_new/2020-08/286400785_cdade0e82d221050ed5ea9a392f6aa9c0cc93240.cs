using OnlineMarks.Data.Models;
using OnlineMarks.Data.ViewModels.Users;
using OnlineMarks.Interfaces.Maps;
using System;
using System.Collections.Generic;
using System.Linq;

namespace OnlineMarks.Maps.UserMap
{
    public class UserViewUserMap : IUserViewUserMap
    {
        public UserView Translate(User user)
        {
            if (user == null)
                return null;

            var temp = new UserView() { Name = user.UserName };
            return temp;
        }

        public IEnumerable<UserView> Translate(IEnumerable<User> users)
        {
            if (users == null)
                return null;

            return users.Select(x => Translate(x));
        }
    }
}