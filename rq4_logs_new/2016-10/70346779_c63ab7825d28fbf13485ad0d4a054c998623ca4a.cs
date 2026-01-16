using MantenedoresCRUD.dataBase;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MantenedoresCRUD.modelo;
using Oracle.ManagedDataAccess.Types;

namespace MantenedoresCRUD.dao
{
    class ComponenteDAO
    {
        private static Data conn;

        public ComponenteDAO()
        {
            conn = Data.getInstance();
        }
        public DataSet ListarComponentes(Aeronave nave)
        {
            DataSet ds = new DataSet();
            if (conn.Open())
            {
                OracleCommand oraCmd = new OracleCommand(conn.getUsuario() + "COMPONENTE_BY_MATRICULA", conn.Cnn);
                oraCmd.BindByName = true;
                oraCmd.CommandType = CommandType.StoredProcedure;
                oraCmd.Parameters.Add("matricula_nave_var", OracleDbType.Varchar2, nave.Matricula, ParameterDirection.Input);
                oraCmd.Parameters.Add("p_recordset", OracleDbType.RefCursor, ParameterDirection.Output);
                oraCmd.ExecuteNonQuery();
                OracleDataAdapter ad = new OracleDataAdapter(oraCmd);
                ad.Fill(ds, "listaComponentes", (OracleRefCursor)(oraCmd.Parameters["p_recordset"].Value));
                conn.Close();
            }
            return ds;
        }

    }
}