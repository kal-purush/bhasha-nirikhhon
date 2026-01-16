using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ICT4Rails.Data
{
    public class TramQueries : ITramContext
    {
        public List<string> GetTrams()
        {
            var trams = new List<string>();
            using (var database = DbConnection.Connection)
            using (var command = database.CreateCommand())
            {
                command.CommandText = "SELECT t.TramID, t.TYPE, s.Name, t.Lenght " +
                                      "FROM TRAM t left outer join Status_has_Tram st " +
                                      "on(t.TramID = st.TramID) " +
                                      "left outer join STATUS s " +
                                      "on(st.StatusID = s.StatusID) " +
                                      "order by t.TramID";

                try
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                var values = Convert.ToString(reader["TramID"]) + "," + Convert.ToString(reader["Type"]) + ","
                                     + Convert.ToString(reader["Name"]) + "," + Convert.ToString(reader["Lenght"]);
                                trams.Add(values);
                            }
                        }
                        return trams;
                    }
                }
                catch (Exception)
                {
                    return null;
                }
            }
        }
       
    }
}