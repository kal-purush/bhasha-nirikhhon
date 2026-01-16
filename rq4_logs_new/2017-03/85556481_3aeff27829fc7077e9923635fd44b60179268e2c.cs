using Modulewijzer.Models;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;


namespace Modulewijzer.Data
{
    public sealed class TableModuleTeacher
    {
        /// <summary>
        /// Inserts the given moduleteacher into the database.
        /// </summary>
        /// <param name="moduleteacher">The moduleteacher to insert.</param>
        public void Create(ModuleTeacher moduleteacher)
        {
            // TODO: Place connection string somewhere else.
            using (var connection = new SqlConnection("Data Source=(LocalDB)\\MSSQLLocalDB;AttachDbFilename=|DataDirectory|Modulewijzer.mdf;Integrated Security=True"))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "INSERT INTO ModuleDocent (ModuleId, DocentId)" +
                        "Values(@ModuleId, @TeacherId)";

                    SqlParameter[] parameters =
                    {
                        new SqlParameter("@ModuleId", SqlDbType.Int) { Value = moduleteacher.ModuleId },
                        new SqlParameter("@TeacherId", SqlDbType.Int) {Value = moduleteacher.TeacherId },
                    };
                    command.Parameters.AddRange(parameters);

                    command.ExecuteNonQuery();
                }

                connection.Close();
            }

        }

        /// <summary>
        /// Deletes the given moduleteacher from the database.
        /// </summary>
        /// <param name="moduleteacher">The moduleteacher to delete.</param>
        public void Delete(ModuleTeacher moduleteacher)
        {

        }
    }
}