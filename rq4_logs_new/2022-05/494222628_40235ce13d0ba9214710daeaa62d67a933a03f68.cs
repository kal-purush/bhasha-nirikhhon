using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Models;
using Microsoft.Data.SqlClient;

namespace DL
{
    public class UserRepo : IUserRepo
    {
        public User SearchUser(User user, string connectionString)
        {
            throw new NotImplementedException();
        }

        public List<User> SearchUsers(string connectionString)
        {
            throw new NotImplementedException();
        }

        public bool AddUser(User user, string connectionString)
        {
            string commandString = $"INSERT INTO Users(UserName, UserEmail, UserPassword, UserPhone) VALUES('{user.Name}', '{user.Email}', '{user.Password}', '{user.Phone}'); ";




            using SqlConnection connection = new(connectionString);
            connection.Open();
            using SqlCommand command = new(commandString, connection);
            //Console.WriteLine(command);
            using SqlDataReader reader = command.ExecuteReader();

            return true;
            throw new NotImplementedException();
        }


    }
}