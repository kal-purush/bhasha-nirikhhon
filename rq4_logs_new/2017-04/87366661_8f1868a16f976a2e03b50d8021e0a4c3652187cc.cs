using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Web;
using System.Web.Services;
using Utilities;

namespace CloudService
{
    /// <summary>
    /// Summary description for CloudSVC
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    // [System.Web.Script.Services.ScriptService]
    public class CloudSVC : System.Web.Services.WebService
    {

        [WebMethod]
        public int attemptLogin(string[] loginInfo)
        {
            int response;
            string email = loginInfo[0];
            string password = loginInfo[1];
            DBConnect objDB;
            SqlCommand objCommand;
            DataSet myDs;
 

            objDB = new DBConnect();
            objCommand = new SqlCommand();
            objCommand.CommandType = CommandType.StoredProcedure;
            objCommand.CommandText = "GetCloudUserInfo";
            objCommand.Parameters.AddWithValue("@email", email);
            myDs = objDB.GetDataSetUsingCmdObj(objCommand);

            if (myDs.Tables[0].Rows.Count != 0)
            {
                if (password != objDB.GetField("Password", 0).ToString())
                {
                    response = 0;
                }
                else
                {
                    response = 1;
                }
            }
            else
            {
                objDB = new DBConnect();
                objCommand = new SqlCommand();
                objCommand.CommandType = CommandType.StoredProcedure;
                objCommand.CommandText = "GetCloudAdminInfo";
                objCommand.Parameters.AddWithValue("@email", email);
                myDs = objDB.GetDataSetUsingCmdObj(objCommand);

                if (myDs.Tables[0].Rows.Count != 0)
                {
                    if (password != objDB.GetField("Password", 0).ToString())
                    {
                        response = 0;
                    }
                    else
                    {
                        response = 2;
                    }
                }
                else
                {
                    response = -1;
                }
            }      
            return response;
        }

        [WebMethod]
        public int addUser(string[] userInfo)
        {
            int response;
            string name = userInfo[0];
            string email = userInfo[1];
            string password = userInfo[2];
            string phone = userInfo[3];
            DBConnect objDB;
            SqlCommand objCommand;
            DataSet myDs;

            objDB = new DBConnect();
            objCommand = new SqlCommand();
            objCommand.CommandType = CommandType.StoredProcedure;
            objCommand.CommandText = "GetCloudUserInfo";
            objCommand.Parameters.AddWithValue("@email", email);
            myDs = objDB.GetDataSetUsingCmdObj(objCommand);

            if (myDs.Tables[0].Rows.Count != 0)
            {
                response = -1;
            }
            else
            {
                objDB = new DBConnect();
                objCommand = new SqlCommand();
                objCommand.CommandType = CommandType.StoredProcedure;
                objCommand.CommandText = "AddCloudUser";
                objCommand.Parameters.AddWithValue("@name", name);
                objCommand.Parameters.AddWithValue("@email", email);
                objCommand.Parameters.AddWithValue("@password", password);
                objCommand.Parameters.AddWithValue("@phone", phone);
                myDs = objDB.GetDataSetUsingCmdObj(objCommand);
                response = 0;
            }

            
            return response;
        }

    }
}