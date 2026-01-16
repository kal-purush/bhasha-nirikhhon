using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.Windows.Forms;

namespace chocoflantastico
{
    class Usuario
    {
        private string user;
        private string pass;

        public string User { get => user; set => user = value; }
        public string Pass { get => pass; set => pass = value; }

        public SqlConnection connection = new SqlConnection("Data Source = localhost; Initial Catalog = chocoflantastico; Integrated Security = True");

        public bool Autenticacion(string comando)
        {
            SqlCommand command = new SqlCommand(comando, connection);
            command.Parameters.AddWithValue("@user", user);
            command.Parameters.AddWithValue("@pass", pass);
            SqlDataAdapter adapter = new SqlDataAdapter(command);
            DataTable table = new DataTable();
            adapter.Fill(table);

            if (table.Rows.Count == 1)
            {
                if (table.Rows[0][0].ToString() == "1")
                {
                    user = table.Rows[0][1].ToString();
                    new FormGeneralRoot(table.Rows[0][1].ToString()).Show();
                }
                else
                {
                    user = table.Rows[0][1].ToString();
                    new FormGeneral(table.Rows[0][1].ToString()).Show();
                }

                return true;
            }
            else
            {
                return false;
            }
        }
    }
}