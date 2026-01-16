using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ADONetExercise
{
    class DBShop
    {
        private string _connectionString;

        public DBShop(string connestionString)
        {
            _connectionString = connestionString;
        }

        public int GetProductCount()
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                var sql = "SELECT COUNT(Name) FROM [dbo].[Product];";

                using (var command = new SqlCommand(sql, connection))
                {
                    return (int)command.ExecuteScalar();
                }
            }
        }

        public void CreateProduct(string name, int categoryId)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                var sql = "INSERT INTO [dbo].[Product](Name, Category_Id)" +
                    " VALUES(@name, @categoryId);";

                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.Add(new SqlParameter("@name", name) { SqlDbType = SqlDbType.NVarChar });

                    command.Parameters.Add(new SqlParameter("@categoryId", categoryId) { SqlDbType = SqlDbType.Int });

                    command.ExecuteNonQuery();
                }
            }
        }

        public void CreateCategory(string name)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                var sql = "INSERT INTO [dbo].[Category1](Name)" +
                    " VALUES(@name);";

                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.Add(new SqlParameter("@name", name) { SqlDbType = SqlDbType.NVarChar });

                    command.ExecuteNonQuery();
                }
            }
        }

        public void UpdateProduct(string newName, int newCategoryId, string condition)
        {
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                var sql = "UPDATE [dbo].[Product] SET Name = @name, Category_Id = @categoryId WHERE @condition;";

                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.Add(new SqlParameter("@name", newName) { SqlDbType = SqlDbType.NVarChar });

                    command.Parameters.Add(new SqlParameter("@categoryId", newCategoryId) { SqlDbType = SqlDbType.Int });

                    command.Parameters.Add(new SqlParameter("@condition", condition) { SqlDbType = SqlDbType.Text });

                    command.ExecuteNonQuery();
                }
            }
        }
    }
}