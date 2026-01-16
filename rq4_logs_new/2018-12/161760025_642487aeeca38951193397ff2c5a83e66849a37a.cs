using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using SEP_framwork.Models.HandleModel;

namespace SEP_framwork.Controllers.HandleController
{
    public class HandleController
    {
        private HandleData hdl_data;
        private string _urlDB;

        public DataTable ReadData(string nameTable)
        {
            string sql = "select * from " + nameTable;
            DataTable result = this.hdl_data.getData(sql);
            return result;
        }

        /*protected bool AddData()
        {
            sql = "insert ";
            return true;
        }*/
        //protected bool UpdateData()
        //{
        //    sql = "insert ";
        //    return true;
        //}
        //protected bool DeleteData()
        //{
        //    sql = "insert ";
        //    return true;
        //}
        
        public HandleController(string url)
        {
            this.hdl_data = new HandleData(url);
            this._urlDB = url;
        }
    }
}
