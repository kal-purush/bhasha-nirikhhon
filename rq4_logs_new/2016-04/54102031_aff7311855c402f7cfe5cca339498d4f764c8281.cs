using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ICT4Rails.Models;
using ICT4Rails.Models.Users;

namespace ICT4Rails.Data
{
    class ReservationQueries
    {
        public List<Reservation> GetReservations(List<Tram> trams, List<Segment> segments)
        {
            List<Reservation> reservations = new List<Reservation>();
            Tram addtram = null;
            Segment addsegment = null;
            DateTime begindate;
            DateTime enddate;
            using (var database = DbConnection.Connection)
            using (var command = database.CreateCommand())
            {
                command.CommandText = "SELECT * " +
                                      "FROM RESERVATION";

                try
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                foreach(Tram tram in trams)
                                {
                                    if(Convert.ToString(reader["TramID"]) == tram.TramID)
                                    {
                                        addtram = tram;
                                    }
                                }

                                foreach(Segment segment in segments)
                                {
                                    if(Convert.ToString(reader["SegmentId"]) == segment.Name)
                                    {
                                        addsegment = segment;
                                    }
                                }

                                string value = Convert.ToString(reader["Begindate"]);
                                value = value.Substring(0, 9);
                                string[] values = value.Split('-');
                                begindate = new DateTime(Convert.ToInt32(values[2]), Convert.ToInt32(values[1]), Convert.ToInt32(values[0]));

                                string value2 = Convert.ToString(reader["Enddate"]);
                                value2 = value2.Substring(0, 9);
                                string[] values2 = value.Split('-');
                                enddate = new DateTime(Convert.ToInt32(values2[2]), Convert.ToInt32(values2[1]), Convert.ToInt32(values2[0]));

                                Reservation reservation = new Reservation(begindate, enddate, addtram, addsegment);
                                reservations.Add(reservation);
                            }
                        }
                        return reservations;
                    }
                }
                catch (Exception)
                {
                    return null;
                }
            }
        }

        public void RemoveReservation(string segmentid)
        {
            using (var database = DbConnection.Connection)
            using (var command = database.CreateCommand())
            {
                command.CommandText = "DELETE FROM Reservation " +
                                      "WHERE SegmentID = " + @segmentid;

                try
                {
                    command.ExecuteNonQuery();
                }
                catch (Exception)
                {

                }
            }
        }

        public void AddReservation(string tramid, string segmentid, DateTime begindate, DateTime enddate)
        {
            using (var database = DbConnection.Connection)
            using (var command = database.CreateCommand())
            {
                command.CommandText = "INSERT INTO Reservation (TramID, SegmentID, Begindate, Enddate) " +
                                      "VALUES ("+ @tramid + ", " + @segmentid + ", to_date("+'"'+ @begindate.ToString("yyyy-MM-dd HH:mm:ss") + '"' + ", 'yyyy-mm-dd hh24:mi:ss')" + ", to_date("+'"'+ @enddate.ToString("yyyy-MM-dd HH:mm:ss") + '"' + ", 'yyyy-mm-dd hh24:mi:ss')";

                try
                {
                    command.ExecuteNonQuery();
                }
                catch (Exception)
                {

                }
            }
        }
    }
}