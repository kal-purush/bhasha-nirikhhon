using BELBModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BELBBL
{
    public interface IUserBL
    {
        /// <summary>
        /// Adds a user to the database
        /// </summary>
        /// <param name="u">User u to add to database</param>
        /// <returns>Id string of user</returns>
        Task<string> AddUser(User u);
        /// <summary>
        /// Given a user's authId, gets their entry in the db
        /// </summary>
        /// <param name="Id">AuthId of user</param>
        /// <returns>User with associated Id</returns>
        Task<User> GetUser(string Id);
    }
}