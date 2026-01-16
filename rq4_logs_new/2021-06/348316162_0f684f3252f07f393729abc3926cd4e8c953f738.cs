using EventPlanner.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Windows;
using System.Windows.Resources;

namespace EventPlanner.Services
{
    class UserService
    {
        private readonly string PATH = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), @"Data\users.json");
        public static UserService Singleton()
        {
            return singleton ??= new UserService();
        }
        public User CurrentUser { get { return GetCurrentUser(); } }
        public bool Login(string username, string password)
        {
            User user = GetUsers().SingleOrDefault(user => user.Username.Equals(username) && user.Password.Equals(password));
            this.username = user?.Username ?? string.Empty;

            return this.username.Length != 0;
        }

        public void Logout()
        {
            username = string.Empty;
        }

        private User GetCurrentUser()
        {
            User user = GetUsers().SingleOrDefault(user => user.Username.Equals(username));
            if (user != null)
            {
                user.Conversations =  ConversationService.Singleton().GetUsersConversations(user);
            }

            return user;
        }
        public User GetUserInfo(int id)
        {
            return GetUsers().FirstOrDefault(user => user.ID == id);
        }
        public List<User> GetUsers()
        {
            List<User> users = new List<User>();
            using (StreamReader reader = new StreamReader(PATH))
            {
                string data = reader.ReadToEnd();
                users = JsonConvert.DeserializeObject<List<User>>(data);
            }
            return users;
        }

        private static UserService singleton = null;
        private string username;
    }
}