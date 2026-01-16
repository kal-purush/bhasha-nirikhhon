using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ICT4Rails.Models;
using ICT4Rails.Models.Users;
using ICT4Rails.Models.Enums;

namespace ICT4Rails.Data
{
    class MaintenanceQueries
    {
        public List<Maintenance> GetMaintenance(List<Tram> trams, List<User> users)
        {
            List<Maintenance> maintenances = new List<Maintenance>();
            List<User> workers = null;
            Tram addtram = null;
            MaintenanceType maintenancetype = MaintenanceType.Cleaning;
            DateTime Mdate;

            using (var database = DbConnection.Connection)
            using (var command = database.CreateCommand())
            {
                command.CommandText = "SELECT m.*, um.* " + 
                                      "FROM Maintenance m LEFT OUTER JOIN User_Maintenance um "+
                                      "ON (m.MaintenanceID = um.MaintenanceID)";

                try
                {
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                foreach(User user in users)
                                {
                                    if(user.UserID == Convert.ToInt32(reader["UserId"]))
                                    {
                                        var adduser = user;
                                        workers = new List<User>();
                                        workers.Add(adduser);
                                    }
                                }

                                foreach(Tram tram in trams)
                                {
                                    if(tram.TramID == Convert.ToString(reader["TramID"]))
                                    {
                                        addtram = tram;
                                    }
                                }

                                if (Convert.ToString(reader["Type"]) == "Kleine schoonmaakbeurt" || Convert.ToString(reader["Type"]) == "Grote schoonmaakbeurt")
                                {
                                    maintenancetype = MaintenanceType.Cleaning;
                                }
                                else if (Convert.ToString(reader["Type"]) == "Kleine onderhoudsbeurt" || Convert.ToString(reader["Type"]) == "Grote onderhoudsbeurt")
                                {
                                    maintenancetype = MaintenanceType.Reparation;
                                }

                                string value = Convert.ToString(reader["Mdate"]);
                                value = value.Substring(0, 9);
                                string[] values = value.Split('-');
                                Mdate = new DateTime(Convert.ToInt32(values[2]), Convert.ToInt32(values[1]), Convert.ToInt32(values[0]));

                                var maintenance = new Maintenance(Convert.ToInt32(reader["MaintenanceID"]), addtram, maintenancetype, Convert.ToString(reader["Specification"]), Mdate, Convert.ToInt32(reader["Duration"]), workers);
                                maintenances.Add(maintenance);
                            }
                        }
                        return maintenances;
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