using System;
using System.Data;
using System.Data.SqlClient;


    public class clsDAC
    {
        SqlConnection sqlCon;

    public clsDAC()
    {
        sqlCon = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["Connstr"].ToString());
    }
    public DataSet GetDataSet(string CommandText)
    {
        DataSet ds = new DataSet();
        SqlCommand cmd = new SqlCommand();
        cmd.Connection = sqlCon;
        cmd.CommandText = CommandText;
        cmd.CommandType = CommandType.Text;
        SqlDataAdapter adap = new SqlDataAdapter();
        adap.SelectCommand = cmd;
        adap.Fill(ds);
        return ds;
    }
    public DataTable GetDataTable(string CommandText)
    {
        DataTable dt = new DataTable();
        SqlCommand cmd = new SqlCommand();
        cmd.Connection = sqlCon;
        cmd.CommandText = CommandText;
        cmd.CommandType = CommandType.Text;
        SqlDataAdapter adap = new SqlDataAdapter();
        adap.SelectCommand = cmd;
        adap.Fill(dt);
        return dt;
    }
    public void ExecuteNonQuery(string CommandText)
    {
        try
        {
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = sqlCon;
            cmd.CommandText = CommandText;
            cmd.CommandType = CommandType.Text;
            cmd.Connection = sqlCon;
            sqlCon.Open();
            cmd.ExecuteNonQuery();
            sqlCon.Close();
        }
        catch (Exception ex)
        {
            if (sqlCon.State == ConnectionState.Open)
                sqlCon.Close();
            throw ex;
        }
    }
    }