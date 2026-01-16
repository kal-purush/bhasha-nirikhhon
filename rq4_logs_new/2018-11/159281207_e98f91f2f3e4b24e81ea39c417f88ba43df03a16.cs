using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServiceLayer
{

    /* This TemporaryUserRepo class was created for TESTING/DEMO PURPOSES ONLY
     * The Repostiory Classes are where you do the Create Read Update Delete (CRUD) ops on the database.*/
    class TemporaryUserRepository
    {
        //used to store created users FOR TESTING/DEMO PURPOSES ONLY
        //Its a mock DB
        private Dictionary<string, User> _users { get; set;}

        //This method is to accomplish the read in the crud ops
        public User GetUser(string username)
        {
            //User user;
            //if (_users.TryGetValue(username, out user))
            //{
            //    return user;
            //}


            /*Note: TryGetValue is a dictionary method. click on TryGetValue and then press F12 and Visual studio will take you 
             * to where it is coming from. 
             * When we actually implement we will be using an actual repository class which uses the context to get users from DB.*/

            //This is doing inline declaration and does the same thing as commented code above.
            if (_users.TryGetValue(username, out User user))
            {
                return user;
            }
            return null;
        }

        //look up Contains() to learn what it is doing. This is also a Dictionary method.
        public void CreateUser(User user)
        {
            if (_users.Keys.Contains(user.Username))
            {
                throw new Exception("User already exists");
            }

            _users.Add(user.Username, user);
        }

        public void UpdateUser(User user)
        {
            if (!_users.ContainsKey(user.Username))
            {
                throw new Exception("User not found in database.");
            };

            _users.Add(user.Username, user);
        }

        public void DeleteUser(string username)
        {
            _users.Remove(username);
        }
    }
}