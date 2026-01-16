using System.Data;
using System.Data.SqlClient;

namespace AiChatBot
{
    internal class DataBase
    {
        public static SqlConnection connect = new SqlConnection(@"Server=localhost;Database=master;Trusted_Connection=True;");
        public static string query = "CREATE TABLE CHATSESSIONS(CHAT_ID BIGINT,ID_SESSION char(50))";
        public static async Task CreateTable()
        {
            connect.Open();
            try
            {
                _ = new SqlCommand(query, connect).ExecuteReader();
                Console.WriteLine("Created table CHATSESSIONS.");
                if (connect.State == ConnectionState.Open)
                {
                    connect.Close();
                }
            }
            catch
            {
                Console.WriteLine("Table already exists.");
                // Console.WriteLine(e);
                if (connect.State == ConnectionState.Open)
                {
                    connect.Close();
                }
            }
        }
        public static async Task Query(long chatId)
        {
            connect.Open();
            try
            {
                query = "INSERT INTO CHATSESSIONS(CHAT_ID, ID_SESSION)";
                query += $"VALUES('{chatId}','{DateTime.Now}')";
                _ = new SqlCommand(query, connect).ExecuteReader();
            }
            catch (Exception e)
            {
                Console.WriteLine("Query not initialized.");
                Console.WriteLine(e);
                if (connect.State == ConnectionState.Open)
                {
                    connect.Close();
                }
            }
            
        }
        
    }
}