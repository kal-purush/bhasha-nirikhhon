using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using GreenMonkeysMVC.Models;
using System.Data.SqlClient;

namespace GreenMonkeysMVC.Data
{

    public class ComputerRepository
    {
        public SqlConnection Connection
        {
            get
            {
                string _connectionString = "Server=localhost\\SQLEXPRESS;Database=BangazonWorkforce;Trusted_Connection=True;";
                return new SqlConnection(_connectionString);
            }
        }
        public List<Computer> GetAvailableComputers()
        {

            using (SqlConnection conn = Connection)
            {

                conn.Open();

                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = @"SELECT Id, PurchaseDate, DecomissionDate, Make, Model FROM Computer ";

                    SqlDataReader reader = cmd.ExecuteReader();
                        var computers = new List<Computer>();
                        while (reader.Read())
                        {
                            computers.Add(new Computer
                            {
                                Id = reader.GetInt32(reader.GetOrdinal("Id")),
                                Make = reader.GetString(reader.GetOrdinal("Make")),
                                Model = reader.GetString(reader.GetOrdinal("Model")),
                                PurchaseDate = reader.GetDateTime(reader.GetOrdinal("PurchaseDate")),
                                DecomissionDate = reader.IsDBNull(2) ? null : (DateTime?)reader.GetDateTime(reader.GetOrdinal("DecomissionDate")),
                            });
                        }
                 
                    reader.Close();

                    EmployeeRepository employeeRepo = new EmployeeRepository();
                    List<Employee> allEmployees = employeeRepo.GetAllEmployees();

                    var availableComputers = computers.Where(c => !allEmployees.Any(e => e.ComputerId == c.Id)).ToList();

                    return availableComputers;
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