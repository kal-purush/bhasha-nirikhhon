using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataAcces.Model
{
    public class Commands
    {
        private static List<Person> viewpersondtb;

        public static List<Person> GetEmpFromDTB
        {
            get
            {
                viewpersondtb = new List<Person>();

                using (SqlConnection conn = new SqlConnection(connectionString: @"Data Source=(LocalDB)\MSSQLLocalDB;AttachDbFilename=C:\Users\Jan.Ehrlich\source\repos\My_ASP_Performance\My_ASP_Performance\App_Data\employeesDTB.mdf;Integrated Security=True"))
                {
                    using (SqlCommand cmd = new SqlCommand("SELECT * FROM empDTB", conn))
                    {
                        conn.Open();
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Person dtb = new Person();
                                dtb.Id = reader.GetInt32(0);
                                dtb.FirstName = reader.GetString(1);
                                dtb.LastName = reader.GetString(2);
                                dtb.Email = reader.GetString(3);
                                dtb.Birth = reader.GetInt32(4);
                                dtb.Registred = reader.GetDateTime(5);

                                viewpersondtb.Add(dtb);
                            }
                        }
                    }
                }
                return viewpersondtb;
            }
        }
        public static void AddEmpToDTB(Person p)
        {
            using (SqlConnection conn = new SqlConnection(connectionString: @"Data Source=(LocalDB)\MSSQLLocalDB;AttachDbFilename=C:\Users\Jan.Ehrlich\source\repos\My_ASP_Performance\My_ASP_Performance\App_Data\employeesDTB.mdf;Integrated Security=True"))
            {
                SqlCommand cmd = new SqlCommand("INSERT INTO empDTB(FirstName, LastName, Email, Birth, Registred) VALUES(@1, @2, @3, @4, @5)", conn);
                conn.Open();
                cmd.Parameters.AddWithValue("@1", p.FirstName);
                cmd.Parameters.AddWithValue("@2", p.LastName);
                cmd.Parameters.AddWithValue("@3", p.Email);
                cmd.Parameters.AddWithValue("@4", p.Birth);
                cmd.Parameters.AddWithValue("@5", p.Registred);

                cmd.ExecuteNonQuery();
            }
        }
    }
}