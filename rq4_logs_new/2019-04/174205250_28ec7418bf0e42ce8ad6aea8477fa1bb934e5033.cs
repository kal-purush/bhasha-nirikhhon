using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Windows.Forms;

namespace RSdbconnection
{
     class DBConnect
     {
          private MySqlConnection connection;
          private string server;
          private string database;
          private string uid;
          private string password;
          private string port;

          //Constructor
          public DBConnect()
          {
               Initialize();
          }

          //Initialize values
          private void Initialize()
          {
               server = "rsinstance.cfwuqmgvld6f.us-east-1.rds.amazonaws.com";
               port = "3306";
               database = "RSdatabase";
               uid = "cis375";
               password = "railwaysystem";
               string connectionString;
               connectionString = "SERVER=" + server + ";" + "PORT=" + port + ";" + "DATABASE=" +
                 database + ";" + "UID=" + uid + ";" + "PASSWORD=" + password + ";";

               connection = new MySqlConnection(connectionString);
          }

          //open connection to database
          private bool OpenConnection()
          {
               try
               {
                    connection.Open();
                    return true;
               }
               catch (MySqlException ex)
               {
                    //When handling errors, you can your application's response based 
                    //on the error number.
                    //The two most common error numbers when connecting are as follows:
                    //0: Cannot connect to server.
                    //1045: Invalid user name and/or password.
                    switch (ex.Number)
                    {
                         case 0:
                              MessageBox.Show("Cannot connect to server.  Contact administrator");
                              break;

                         case 1045:
                              MessageBox.Show("Invalid username/password, please try again");
                              break;
                    }
                    return false;
               }
          }

          //Close connection
          private bool CloseConnection()
          {
            try
            {
                connection.Close();
                return true;
            }
            catch (MySqlException ex)
            {
                MessageBox.Show(ex.Message);
                return false;
            }
          }

          //Insert statement
          public void Insert()
          {

          }

          //Update statement
          public void Update()
          {
          }

          //Delete statement
          public void Delete()
          {
          }

          //Select statement
          public List<string>[] Select()
          {
          }

          //Count statement
          public int Count()
          {
          }

          //Backup
          public void Backup()
          {
          }

          //Restore
          public void Restore()
          {
          }
     }
}