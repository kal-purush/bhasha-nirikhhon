using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ICT4Rails.Data
{
    public class UserQueries : IUserContext
    {
        public List<string> GetUsers()
        {
            var users = new List<string>();
            using (var database = DbConnection.Connection)
            using (var command = database.CreateCommand())
            {
                command.CommandText = "SELECT * " +
                                      "FROM "+'"'+ "User" + '"';

                try
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                var values = Convert.ToString(reader["UserID"]) + "," 
                                     + Convert.ToString(reader["Username"]) + ","
                                     + Convert.ToString(reader["Password"]) + ","
                                     + Convert.ToString(reader["Age"]) + ","
                                     + Convert.ToString(reader["Profession"]) + ","
                                     + Convert.ToString(reader["Firstname"]) + ","
                                     + Convert.ToString(reader["Surname"]) + ","
                                     + Convert.ToString(reader["Surnameprefix"]) + ","
                                     + Convert.ToString(reader["Email"]) + ",";
                                users.Add(values);
                            }
                        }
                        return users;
                    }
                }
                catch (Exception)
                {
                    return null;
                }
            }
        }
    }
}