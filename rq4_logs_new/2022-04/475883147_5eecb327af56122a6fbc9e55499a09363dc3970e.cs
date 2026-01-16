using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;

namespace DAL
{
    public class AddCustomerToDB
    {

        DAO dao = new DAO();
        public void AddCustomer(string fn, string sn, string email,string phone, string add1, 
            string add2, string city, string cy,string AccType, int acc_no,int Scode,int IBal,
            decimal Amount,int ODraft)
        {

            SqlCommand cmd = dao.OpenCon().CreateCommand();

            cmd.CommandText = "uspAddCustomer";
            cmd.CommandType = CommandType.StoredProcedure;

            cmd.Parameters.AddWithValue("@fn",fn);
            cmd.Parameters.AddWithValue("@sn",sn);
            cmd.Parameters.AddWithValue("@email", email);
            cmd.Parameters.AddWithValue("@ph",phone);
            cmd.Parameters.AddWithValue("@add1", add1);
            cmd.Parameters.AddWithValue("@add2", add2);
            cmd.Parameters.AddWithValue("@city", city);
            cmd.Parameters.AddWithValue("@cy", cy);
            cmd.Parameters.AddWithValue("@AccType",AccType);
            cmd.Parameters.AddWithValue("@AccNo", acc_no);
            cmd.Parameters.AddWithValue("@Scode",Scode);
            cmd.Parameters.AddWithValue("@IBal",IBal);
            cmd.Parameters.AddWithValue("@am",Amount);
            cmd.Parameters.AddWithValue("@ODraft",ODraft);

            cmd.ExecuteNonQuery();
            dao.CloseCon();
        }






    }
}