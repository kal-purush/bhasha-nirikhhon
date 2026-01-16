using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;

namespace Payroll_Service_Ado
{
    public class PayrollRepository
    {
        public static string connectionString = @"Server=MUKESH\SQLEXPRESS; Initial Catalog =payroll_service;;Integrated Security=True;Connect Timeout=30;
Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False";
        SqlConnection connection = new SqlConnection(connectionString);

        public bool CheckConnection()
        {
           
            try
            {
            // If no error return true else false
            connection.Open();
            connection.Close();
            
            return true;
            }
            catch
            { 
                return false;
            }
        }
    }
}