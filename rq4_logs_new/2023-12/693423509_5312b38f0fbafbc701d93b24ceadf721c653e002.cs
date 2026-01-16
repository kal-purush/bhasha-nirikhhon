using Npgsql;
using QLSV.DTO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static QLSV.DAO.AccoutManagementDAO;

namespace QLSV.DAO
{
    internal class AccoutManagementDAO
    {
        public class AccountManagementDAO
        {
            private static AccountManagementDAO instance;

            public static AccountManagementDAO Instance
            {
                get
                {
                    if (instance == null) instance = new AccountManagementDAO();
                    return AccountManagementDAO.instance;
                }
                private set
                {
                    AccountManagementDAO.instance = value;
                }
            }

            public AccountManagementDAO() { }
            private string HashPassword(string password)
            {

                return BCrypt.Net.BCrypt.EnhancedHashPassword(password, 12);
            }

            public bool AddAccount(AccountManagementDTO account)
            {
                try
                {
                    string query = "INSERT INTO Account (Username, Password) VALUES (@username, @password)";
                    using (NpgsqlConnection connection = new NpgsqlConnection(DataProvider.Instance.connectionStr))
                    {
                        connection.Open();
                        using (NpgsqlCommand command = new NpgsqlCommand(query, connection))
                        {
                            string hashedPassword = HashPassword(account.Password);

                            command.Parameters.AddWithValue("@username", account.Username);
                            command.Parameters.AddWithValue("@password", account.Password);

                            int rowsAffected = command.ExecuteNonQuery();

                            return rowsAffected > 0;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                    return false;
                }
            }
            public bool UpdateAccount(AccountManagementDTO account)
            {
                try
                {
                    string query = "UPDATE Account SET Password = @password WHERE Username = @username";
                    using (NpgsqlConnection connection = new NpgsqlConnection(DataProvider.Instance.connectionStr))
                    {
                        connection.Open();
                        using (NpgsqlCommand command = new NpgsqlCommand(query, connection))
                        {
                            string hashedPassword = HashPassword(account.Password);

                            command.Parameters.AddWithValue("@password", account.Password);
                            command.Parameters.AddWithValue("@username", account.Username);

                            int rowsAffected = command.ExecuteNonQuery();

                            return rowsAffected > 0;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                    return false;
                }

            }

            public bool DeleteAccount(string username)
            {
                try
                {
                    string query = "DELETE FROM Account WHERE Username = @username";
                    using (NpgsqlConnection connection = new NpgsqlConnection(DataProvider.Instance.connectionStr))
                    {
                        connection.Open();
                        using (NpgsqlCommand command = new NpgsqlCommand(query, connection))
                        {
                            command.Parameters.AddWithValue("@username", username);

                            int rowsAffected = command.ExecuteNonQuery();

                            return rowsAffected > 0;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                    return false;
                }
            }
            public AccountManagementDTO SearchAccount(string username)
            {
                try
                {
                    string query = "SELECT * FROM Account WHERE Username = @username";
                    DataTable data = DataProvider.Instance.ExcuteQuery(query, new object[] { username });

                    if (data.Rows.Count > 0)
                    {
                        DataRow row = data.Rows[0];
                        AccountManagementDTO account = new AccountManagementDTO
                        {
                            Username = row["username"].ToString(),
                            Password = row["password"].ToString()
                            // Cập nhật các thông tin khác nếu có
                        };
                        return account;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                }

                return null;
            }
            public DataTable GetAccountData()
            {
                try
                {
                    string query = "SELECT * FROM Account "; // Thay doi query tuy theo cau truc cua bang Account

                    // Su dung DataProvider de thuc hien truy van va lay du lieu
                    DataTable data = DataProvider.Instance.ExcuteQuery(query);

                    return data;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                    return null;
                }
            }



        }
    }
}