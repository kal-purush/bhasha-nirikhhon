using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Darkec.Helpers;
using Darkec.Models;

namespace Darkec.Services.Users
{
    public class UserRepository : IObjectRepository<int, User>
    {
        private string JsonFileName = @".\wwwroot\Data\JsonUsers.json";

        private Dictionary<int, User> users;

        public UserRepository()
        {
            users = JsonFileReader<int, User>.ReadJson(JsonFileName);
            if (users == null)
            {
                users = new Dictionary<int, User>();
            }
        }

        public Dictionary<int, User> GetAllObjects()
        {
            return users;
        }

        public void AddObject(User user)
        {
            AutoIncrementId(user);
            users.Add(user.Id, user);
            JsonFileWriter<int, User>.WriteToJson(users, JsonFileName);
        }

        public User GetObject(int id)
        {
            return users[id];
        }

        public void UpdateObject(User user)
        {
            if (user != null)
            {
                users[user.Id] = user;
                JsonFileWriter<int, User>.WriteToJson(users, JsonFileName);
            }
        }

        public void DeleteObject(User user)
        {
            if (user != null)
            {
                users.Remove(user.Id);
                JsonFileWriter<int, User>.WriteToJson(users, JsonFileName);
            }
        }

        public Dictionary<int, User> FilterObjects(string email)
        {

            Dictionary<int, User> filteredUsers = new Dictionary<int, User>();

            foreach (User user in users.Values)
            {
                if (user.Email.StartsWith(email))
                {
                    filteredUsers.Add(user.Id, user);
                }
            }
            return filteredUsers;
        }
        public User AutoIncrementId(User user)
        {
            List<int> userId = new List<int>();
            foreach (var User in users.Values)
            {
                userId.Add(User.Id);
            }
            if (userId.Count != 0)
            {
                int increment = userId.Max() + 1;
                user.Id = increment;
            }
            else
            {
                user.Id = 1;
            }
            return user;
        }
    }
}