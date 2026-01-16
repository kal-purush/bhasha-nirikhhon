using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Transactions;
using System.Data;

namespace CIS560
{
    /// <summary>
    /// Handles the creation and retrieval of all address entries in the database.
    /// </summary>
    class AddressRepository : IAddressRepository
    {
        SqlConnectionStringBuilder sqlBuilder = SqlBuilder.SqlConnectingStringBuilder;

        /// <summary>
        /// Creates a new address with the given parameters and inserts it into the database.
        /// </summary>
        /// <param name="line1">The first line of the address</param>
        /// <param name="line2">The second line of the address</param>
        /// <param name="city">The city of the address</param>
        /// <param name="state">The state of the address</param>
        /// <param name="zip">The zipcode of the address</param>
        /// <returns>A new <see cref="Address"/> if successful, otherwise <code>null</code></returns>
        public Address CreateAddress(string line1, string line2, string city, string state, int zip)
        {
            const string sqlCommandText = "CIS560.CreateAddress";

            try
            {
                using (TransactionScope transaction = new TransactionScope())
                {
                    using (SqlConnection connection = new SqlConnection(sqlBuilder.ConnectionString))
                    {
                        using (SqlCommand command = new SqlCommand(sqlCommandText, connection))
                        {
                            command.CommandType = CommandType.StoredProcedure;

                            command.Parameters.AddWithValue("Line1", line1);
                            command.Parameters.AddWithValue("Line2", line2);
                            command.Parameters.AddWithValue("City", city);
                            command.Parameters.AddWithValue("State", state);
                            command.Parameters.AddWithValue("Zip", zip);

                            SqlParameter param = command.Parameters.Add("AddressID", SqlDbType.Int);
                            param.Direction = ParameterDirection.Output;

                            connection.Open();

                            command.ExecuteNonQuery();

                            transaction.Complete();

                            return new Address((int)param.Value, line1, line2, city, state, zip);
                        }
                    }
                }
            }
            catch(TransactionAbortedException ex)
            {
                Console.WriteLine("Could not create address. Exited with: {0}", ex.Message);
                return null;
            }
        }

        /// <summary>
        /// Gets an address with the given <code>addressId</code>
        /// </summary>
        /// <param name="addressId">The ID of the desired address</param>
        /// <returns>
        /// If found, an <see cref="Address"/> corresponding to the given <code>addressId</code>, otherwise <code>null</code>.
        /// </returns>
        public Address GetAddress(int addressId)
        {
            //TODO: Set command
            string sqlCommandText = "CIS560.GetAddress";
            using (SqlConnection connection = new SqlConnection(sqlBuilder.ConnectionString))
            {
                using (SqlCommand command = new SqlCommand(sqlCommandText, connection))
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.Parameters.AddWithValue("AddressID", addressId);
                    connection.Open();

                    SqlDataReader reader = command.ExecuteReader();

                    if(!reader.Read())
                    {
                        return null;
                    }

                    return new Address(
                        addressId, 
                        reader.GetString(reader.GetOrdinal("Line1")), 
                        reader.GetString(reader.GetOrdinal("Line2")), 
                        reader.GetString(reader.GetOrdinal("City")), 
                        reader.GetString(reader.GetOrdinal("State")), 
                        reader.GetInt32(reader.GetOrdinal("Zip"))
                    );
                }
            }
        }

        /// <summary>
        /// Gets all the addresses in the table
        /// </summary>
        /// <returns>A <see cref="List{T}"/> of <see cref="Address">Addresses</see>/>.  This may be empty if none are found in the databse.</returns>
        public IReadOnlyList<Address> RetrieveAddresses()
        {
            //TODO: Set command here
            string sqlCommandText = "CIS560.RetrieveAddresses";
            using (SqlConnection connection = new SqlConnection(sqlBuilder.ConnectionString))
            {
                using (SqlCommand command = new SqlCommand(sqlCommandText, connection))
                {
                    command.CommandType = CommandType.StoredProcedure;

                    connection.Open();

                    SqlDataReader reader = command.ExecuteReader();

                    List<Address> addresses = new List<Address>();

                    while(reader.Read())
                    {
                        addresses.Add(new Address(
                            reader.GetInt32(reader.GetOrdinal("AddressID")),
                            reader.GetString(reader.GetOrdinal("Line1")),
                            reader.GetString(reader.GetOrdinal("Line2")),
                            reader.GetString(reader.GetOrdinal("City")),
                            reader.GetString(reader.GetOrdinal("State")),
                            reader.GetInt32(reader.GetOrdinal("Zip"))
                        ));
                    }

                    return addresses;
                }
            }
        }
    }
}