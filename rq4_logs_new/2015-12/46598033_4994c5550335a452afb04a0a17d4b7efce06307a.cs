using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.OleDb;

namespace ebox_client
{
    class DataBaseLayer
    {

        //数据库连接
        string strcon ="";
        OleDbConnection OleCon=null;
        OleDbCommand OleCom = null;
        OleDbDataReader OleReader = null;

        public Boolean AddPkgLogItem(string BoxID)
        {
                //OleCon = new OleDbConnection(strcon);
                //OleCon.Open();
                //string sql = "Select * from BOX_MANAGE where Box_Size='" + size +"' AND Empty_State='0' AND Box_Valid='1'"  ;
                //OleCom = new OleDbCommand(sql, OleCon);
                //OleDbDataReader OleReader = OleCom.ExecuteReader();  

            return true;
        }

    }
}