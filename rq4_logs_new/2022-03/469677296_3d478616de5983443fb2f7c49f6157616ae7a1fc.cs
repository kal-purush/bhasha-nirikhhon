using System;
using System.Data.SqlClient;
using System.IO;
using System.Text.Json;
using System.Windows;

namespace ReportGenerator.Services
{
    /// <summary>
    /// Класс соединения с базой данных
    /// </summary>
    public class DbConnection
    {

        private SqlConnection _connection { get; set; }
        public DbConnection()
        {
            if (File.Exists("settings.json"))
            {
                try
                {
                    string data = File.ReadAllText("settings.json");
                    ConnectConfig connectConfig = JsonSerializer.Deserialize<ConnectConfig>(data);

                    var builder = new SqlConnectionStringBuilder();
                    builder.DataSource = connectConfig.Server;
                    builder.InitialCatalog = connectConfig.DbName;
                    builder.UserID = connectConfig.Username;
                    builder.Password = connectConfig.Password;
                    _connection = new SqlConnection(builder.ToString());
                }
                catch 
                {
                    MessageBox.Show("Ошибка десериализации настроек. Обратитесь к системному администратору.", "Подключение к серверу");
                }
            }            
        }       

        /// <summary>
        /// Возвращает созданное соединение
        /// </summary>        
        public SqlConnection GetConnection()
        {
            return _connection;
        }

        /// <summary>
        /// Открывает соединение и возвращает его или null
        /// </summary>        
        public SqlConnection ConnectOpen()
        {
            try
            {
                _connection.Open();
                return _connection;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// Закрывает соединение
        /// </summary> 
        public void ConnectClose()
        {
            _connection.Close();
        }

        



    }
}