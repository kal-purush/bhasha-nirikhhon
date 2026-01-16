using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.OleDb;
using System.Data;

namespace WindowsFormsApplication1
{
    public class Database
    {
        public OleDbConnection Connec { get; set; }
      
        public void Connection()
        {
            string StringConnection = @"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=C:\Users\Thanh Tuan\Desktop\TypingTest\DataAccount.mdb";
            Connec = new OleDbConnection(StringConnection);
            if (Connec.State != ConnectionState.Open)
            {
                Connec.Open();
            }

        }
         public string GetAcountUsing(OleDbConnection Connec)
        {
            string UseName="";
            string sql = "SELECT UseName FROM TbUser";
            OleDbCommand cmd = new OleDbCommand(sql, Connec);
            OleDbDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                UseName = dr["UseName"].ToString() + "";

            }
            return UseName;
        }

        public int MaxScorePointCharFallOut(OleDbConnection Connec,string UseName)
        {
            string maxscore="";
            int max = 0;

            string sql = "SELECT *FROM TbDataAccount WHERE UseName=Tuan1";

            OleDbCommand cmd = new OleDbCommand(sql, Connec);
            OleDbDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                maxscore = dr["PointCharFallOut"].ToString();
                max = Convert.ToInt32(maxscore);
            }
            return max;
        }
    }

}