using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using GreenMonkeysMVC.Models;
using System.Data.SqlClient;

namespace GreenMonkeysMVC.Data
{

    public class TrainingProgramRepository
    {
        public SqlConnection Connection
        {
            get
            {
                string _connectionString = "Server=localhost\\SQLEXPRESS;Database=BangazonWorkforce;Trusted_Connection=True;";
                return new SqlConnection(_connectionString);
            }
        }
        public List<TrainingProgram> GetTrainingProgramsByEmployeeId(int id)
        {

            using (SqlConnection conn = Connection)
            {

                conn.Open();

                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = @"SELECT tp.Id, tp.Name, tp.StartDate, tp.EndDate, tp.MaxAttendees
                                        FROM EmployeeTraining et
                                        LEFT JOIN TrainingProgram tp
                                        ON et.TrainingProgramId = tp.Id
                                        WHERE et.EmployeeId = @id";

                    cmd.Parameters.Add(new SqlParameter("@id", id));

                    SqlDataReader reader = cmd.ExecuteReader();
                    var trainingPrograms = new List<TrainingProgram>();
                    while (reader.Read())
                    {
                        trainingPrograms.Add(new TrainingProgram
                        {
                            Id = reader.GetInt32(reader.GetOrdinal("Id")),
                            Name = reader.GetString(reader.GetOrdinal("Name")),
                            StartDate = reader.GetDateTime(reader.GetOrdinal("StartDate")),
                            EndDate = reader.GetDateTime(reader.GetOrdinal("EndDate")),
                            MaxAttendees = reader.GetInt32(reader.GetOrdinal("MaxAttendees"))
                        });
                    }

                    reader.Close();

                    return trainingPrograms;
                }
            }
        }


        /*

        /// <summary>
        ///  Returns a single department with the given id.
        /// </summary>
        public Department GetDepartmentById(int id)
        {
            using (SqlConnection conn = Connection)
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT Id, Name, Budget FROM Department WHERE Id = @id";
                    cmd.Parameters.Add(new SqlParameter("@Id", id));
                    SqlDataReader reader = cmd.ExecuteReader();

                    Department department = null;

                    // If we only expect a single row back from the database, we don't need a while loop.
                    if (reader.Read())
                    {
                        department = new Department
                        {
                            Id = id,
                            Name = reader.GetString(reader.GetOrdinal("Name")),
                            Budget = reader.GetInt32(reader.GetOrdinal("Budget"))
                        };
                    }

                    reader.Close();

                    return department;
                }
            }
        }

        /*

        /// <summary>
        ///  Add a new department to the database
        ///   NOTE: This method sends data to the database,
        ///   it does not get anything from the database, so there is nothing to return.
        /// </summary>
        public void AddDepartment(Department department)
        {
            using (SqlConnection conn = Connection)
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    // These SQL parameters are annoying. Why can't we use string interpolation?
                    // ... sql injection attacks!!!
                    cmd.CommandText = "INSERT INTO Department (DeptName) OUTPUT INSERTED.Id Values (@deptName)";
                    cmd.Parameters.Add(new SqlParameter("@deptName", department.DeptName));
                    int id = (int)cmd.ExecuteScalar();

                    department.Id = id;
                }
            }

            // when this method is finished we can look in the database and see the new department.
        }

        /// <summary>
        ///  Updates the department with the given id
        /// </summary>
        public void UpdateDepartment(int id, Department department)
        {
            using (SqlConnection conn = Connection)
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = @"UPDATE Department
                                     SET DeptName = @deptName
                                     WHERE Id = @id";
                    cmd.Parameters.Add(new SqlParameter("@deptName", department.DeptName));
                    cmd.Parameters.Add(new SqlParameter("@id", id));

                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        ///  Delete the department with the given id
        /// </summary>
        public void DeleteDepartment(int id)
        {
            using (SqlConnection conn = Connection)
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "DELETE FROM Department WHERE Id = @id";
                    cmd.Parameters.Add(new SqlParameter("@id", id));
                    cmd.ExecuteNonQuery();
                }
            }
        }

    */

    }
}