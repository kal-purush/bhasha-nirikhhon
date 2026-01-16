using System;
using System.Collections.Generic;

namespace ClientApp
{
    public class ContactList1
    {
        private List<User> users = new List<User>();

        public void AddUser(User user)
        {
            users.Add(user);
        }

        public void RemoveUser(User user)
        {
            users.Remove(user);
        }

        public void PrintAllUsers()
        {
            foreach (User user in users)
            {
                Console.WriteLine($"Name: {user.Name}, Email: {user.Email}, Country: {user.Country}, Phone: {user.Phone}, Passport: {user.Passport}");
            }
        }
    }
}
