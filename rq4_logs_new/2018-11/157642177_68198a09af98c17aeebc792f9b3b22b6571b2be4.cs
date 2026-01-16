using System;
using System.Collections.Generic;
using System.Data;
using System.Transactions;
using System.Data.SqlClient;

namespace CIS560
{
    class DriverRepository : IDriverRepository
    {
        SqlConnectionStringBuilder sqlBuilder = SqlBuilder.SqlConnectingStringBuilder;

        /// <summary>
        /// Creates a driver in the database
        /// </summary>
        /// <param name="firstName">The first name of the driver</param>
        /// <param name="lastName">The last name of the driver</param>
        /// <param name="driversLicenseNumber">The drivers license number of the driver</param>
        /// <returns>The created <see cref="Driver"/> if successful, otherwise <code>null</code></returns>
        public Driver CreateDriver(string firstName, string lastName, string driversLicenseNumber)
        {
            const string sqlCommandText = "CIS560.CreateDriver";

            try
            {
                using (TransactionScope transaction = new TransactionScope())
                {
                    using (SqlConnection connection = new SqlConnection(sqlBuilder.ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(sqlCommandText, connection))
                        {
                            command.CommandType = CommandType.StoredProcedure;

                            command.Parameters.AddWithValue("FirstName", firstName);
                            command.Parameters.AddWithValue("LastName", lastName);
                            command.Parameters.AddWithValue("DriverLicenseNumber", driversLicenseNumber);

                            SqlParameter param = command.Parameters.Add("DriverID", SqlDbType.Int);
                            param.Direction = ParameterDirection.Output;

                            connection.Open();

                            command.ExecuteNonQuery();

                            transaction.Complete();

                            return new Driver((int)param.Value, firstName, lastName, driversLicenseNumber);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Could not create driver.  Exited with: {0}", e.Message);
                return null;
            }
        }

        /// <summary>
        /// Gets the driver with the given <code>driverId</code>
        /// </summary>
        /// <param name="driverId">The ID of the desired driver</param>
        /// <returns>
        /// A <see cref="Driver"/> with the given <code>driverId</code> if found, otherwise <code>null</code>
        /// </returns>
        public Driver GetDriver(int driverId)
        {
            //TODO: Set command
            string sqlCommandText = "CIS560.GetDriver";
            using (SqlConnection connection = new SqlConnection(sqlBuilder.ConnectionString))
            {
                using (SqlCommand command = new SqlCommand(sqlCommandText, connection))
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.Parameters.AddWithValue("DriverID", driverId);
                    connection.Open();

                    SqlDataReader reader = command.ExecuteReader();

                    if (!reader.Read())
                    {
                        return null;
                    }

                    return new Driver(
                        driverId,
                        reader.GetString(reader.GetOrdinal("FirstName")),
                        reader.GetString(reader.GetOrdinal("LastName")),
                        reader.GetString(reader.GetOrdinal("DriversLicenseNumber"))
                    );
                }
            }
        }

        /// <summary>
        /// Gets all the drivers in the database
        /// </summary>
        /// <returns>
        /// A <see cref="List{T}"/> of <see cref="Driver"/>s in the database, or empty if there are none.
        /// </returns>
        public IReadOnlyList<Driver> RetrieveDrivers()
        {
            //TODO: Set command here
            string sqlCommandText = "CIS560.RetrieveDrivers";
            using (SqlConnection connection = new SqlConnection(sqlBuilder.ConnectionString))
            {
                using (SqlCommand command = new SqlCommand(sqlCommandText, connection))
                {
                    command.CommandType = CommandType.StoredProcedure;

                    connection.Open();

                    SqlDataReader reader = command.ExecuteReader();

                    List<Driver> drivers = new List<Driver>();

                    while (reader.Read())
                    {
                        drivers.Add(new Driver(
                            reader.GetInt32(reader.GetOrdinal("DriverID")),
                            reader.GetString(reader.GetOrdinal("FirstName")),
                            reader.GetString(reader.GetOrdinal("LastName")),
                            reader.GetString(reader.GetOrdinal("DriversLicenseNumber"))
                        ));
                    }

                    return drivers;
                }
            }
        }
    }
}