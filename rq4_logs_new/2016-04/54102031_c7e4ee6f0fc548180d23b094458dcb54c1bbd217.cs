using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ICT4Rails.Data
{
    public class SegmentQueries
    {
        public List<string> GetSegments()
        {
            var segments = new List<string>();
            using (var database = DbConnection.Connection)
            using (var command = database.CreateCommand())
            {
                command.CommandText = "SELECT * " +
                                      "FROM " + '"' + "Segment" + '"';

                try
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                var values = Convert.ToString(reader["SegmentID"]) + ","
                                     + Convert.ToString(reader["TrackID"]) + ","
                                     + Convert.ToString(reader["SEGMENT_SegmentID"]) + ","
                                     + Convert.ToString(reader["Blocked"]) + ","
                                     + Convert.ToString(reader["TramID"]);
                                segments.Add(values);
                            }
                        }
                        return segments;
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