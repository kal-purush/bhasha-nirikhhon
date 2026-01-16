using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ReneSqlMessaging
{
    class Program
    {
        static void Main(string[] args)
        {
            SqlConnection connection = new SqlConnection("Server=GMRMLTV;Database=ReneMessaging;User Id=sa;Password=GreatMinds110;");
            connection.Open();

            bool login = false, exit = false;
            Console.WriteLine("Welcome to your reminders app! Leave a message here and check back on it later.");
            while (!login && !exit)
            {
                Console.WriteLine("What would you like to do? [R]egister a new user. [L]ogin. [E]xit.");
                char choice = char.Parse(Console.ReadLine());
                switch (choice)
                {

                    case 'R':
                        {
                            Register(connection);
                            break;
                        }
                    case 'L':
                        {
                            login = Login(connection);
                            if (!login)
                            {
                                Console.WriteLine("Those credentials were incorrect. Please try again.");
                            }
                            else
                            {
                                Console.WriteLine("Welcome!");
                            }
                            break;
                        }
                    case 'E':
                        {
                            exit = true;
                            Console.WriteLine("Have a nice day!");
                            break;
                        }
                }
            }


            connection.Close();
        }

        static void Register(SqlConnection connection)
        {
            SqlCommand registerCommand = new SqlCommand("usp_Register");
            registerCommand.Connection = connection;
            registerCommand.CommandType = System.Data.CommandType.StoredProcedure;
            Console.Write("Username: ");
            string username = Console.ReadLine();
            Console.Write("Password: ");
            string password = Console.ReadLine();
            registerCommand.Parameters.AddWithValue("@username", username);
            registerCommand.Parameters.AddWithValue("@password", password);
            registerCommand.ExecuteNonQuery();
        }

        static bool Login(SqlConnection connection)
        {
            SqlCommand loginCommand = new SqlCommand("usp_Login");
            loginCommand.Connection = connection;
            loginCommand.CommandType = System.Data.CommandType.StoredProcedure;
            Console.Write("Username: ");
            string username = Console.ReadLine();
            Console.Write("Password: ");
            string password = Console.ReadLine();
            loginCommand.Parameters.AddWithValue("@username", username);
            loginCommand.Parameters.AddWithValue("@password", password);
            loginCommand.ExecuteNonQuery();

            SqlDataAdapter adapter = new SqlDataAdapter(loginCommand);
            DataTable table = new DataTable();
            adapter.Fill(table);

            return table.Rows.Count != 0;
        }
    }
}