﻿using System;
using static System.Console;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Collections.Generic;

namespace api_database
{
    public static class DBconnection
    {
        public static (MySqlConnection connection, bool success) Connection(string database = "host1323541_itstep21", string port = "3306", 
            string username = "host1323541_itstep", string pass = "269f43dc", string host = "mysql60.hostland.ru")
        {
            var connString = "Server=" + host + ";Database=" + database + ";port=" + port + ";User Id=" + username + ";password=" + pass;
            var connection = new MySqlConnection(connString);
            connection.Open();
            if (connection.Ping())
            {
                return (connection:connection, success:true);
            }

            return (connection:connection, success:false);
        }
        
        public static Dictionary<string, string> GetSystemMessage() {
            Dictionary<string, string> dictionary = new Dictionary<string, string>();
            var connection = DBconnection.Connection();
            if (connection.success)
            {
                var sql = "SELECT system_message_type, system_message_text FROM t_system_message";
                var result = DBconnection.SelectQuery(sql, connection.connection);
                if(result.HasRows)
                {
                    while (result.Read())
                    {
                        var type = result.GetValue(0);
                        var name = result.GetValue(1);
                        dictionary.Add(type.ToString(), name.ToString());
                    }
                }
            }
            return dictionary;
        }
        
        public static MySqlDataReader SelectQuery(string sql, MySqlConnection connection)
        {
            if (connection.Ping())
            {
                var command = new MySqlCommand { Connection = connection, CommandText = sql };
                var result = command.ExecuteReader();
                if (result != null)
                {
                    return result;
                }
            }
            return null;
        }
        
        public static int InsertQuery(string sql, MySqlConnection connection)
        {
            if (connection.Ping()) 
            {
                var command = new MySqlCommand { Connection = connection, CommandText = sql };
                var result = command.ExecuteNonQuery();
                return result;
            }
            return -1;
        }
        
        public static string Registration (string login, string password)
        {
            var systemMessage = DBconnection.GetSystemMessage();
            var connection = DBconnection.Connection();
            if (connection.success)
            {
                var sql = $"INSERT INTO t_registration(registration_login, registration_password) VALUES ('{login}', '{password}')";
                var result = DBconnection.InsertQuery(sql, connection.connection);
                if (result == 1)
                {
                    return systemMessage["regsuccess"];
                }
                else
                {
                    return systemMessage["regfail"];
                }
            }
            return systemMessage["regfail"];
        }
        
        public static string Authorization (string login, string password)
        {
            var systemMessage = DBconnection.GetSystemMessage();
            var connection = DBconnection.Connection();
            var isAuth = false;
            if (connection.success)
            {
                var sql = "SELECT registration_login, registration_password FROM t_registration";
                var result = DBconnection.SelectQuery(sql, connection.connection);
                if(result.HasRows)
                {
                    while (result.Read())
                    {
                        var baseLogin = result.GetValue(0);
                        var basePassword = result.GetValue(1);
                        if (login == (string) baseLogin && password == (string) basePassword)
                        {
                            isAuth = true;
                        }
                    }
                }

                if (isAuth)
                {
                    return systemMessage["authsuccess"];
                }
                else
                {
                    return systemMessage["authfail"];
                }
            }
            return systemMessage["authfail"];
        }
        
        public static bool PostMessage (string loginSender, string loginRecipient, string textMessage, string typeMessage = "text") {
            var connection = DBconnection.Connection();
            if (connection.success)
            {
                var sql = $"INSERT INTO t_message(message_login_sender, message_login_recipient, message_text, message_type) VALUES ('{loginSender}', '{loginRecipient}', '{textMessage}', '{typeMessage}')";
                var result = DBconnection.InsertQuery(sql, connection.connection);
                if (result == 1)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }

            return false;
        }
        
        public static Dictionary<DateTime, List<string>> GetMessage (string loginSender, string loginRecipient, int quantityMessages)
        {
            var messages = new Dictionary<DateTime,  List<string>>();
            var connection = DBconnection.Connection();
            if (connection.success)
            {
                var sql = $"SELECT message_text, message_date, message_type FROM t_message WHERE message_login_sender = '{loginSender}' and message_login_recipient = '{loginRecipient}' order by message_id desc limit {quantityMessages}";
                var result = DBconnection.SelectQuery(sql, connection.connection);
                if(result.HasRows)
                {
                    while (result.Read())
                    {
                        var listTextType = new List<string>();
                        var baseMessage = result.GetValue(0);
                        var baseDate = result.GetValue(1);
                        var baseType = result.GetValue(2);
                        listTextType.Add(baseMessage.ToString());
                        listTextType.Add(baseType.ToString());
                        messages.Add((DateTime) baseDate, listTextType);
                    }
                }
            }
            return messages;
        }
    }
}