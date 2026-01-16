using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SomerenDAL
{
    public class OrderDao : BaseDao
    {
        public void InsertOrder(int studentId, int drinkId, int orderAmount)
        {
            // Get the maximum OrderID currently in the table
            string queryMaxId = "SELECT MAX(OrderID) FROM dbo.ORDERS";
            DataTable dataTable = ExecuteSelectQuery(queryMaxId, new SqlParameter[0]);
            int maxId = (dataTable.Rows[0][0] is DBNull) ? 0 : Convert.ToInt32(dataTable.Rows[0][0]);

            // Create the SQL query
            string query = "INSERT INTO dbo.ORDERS (OrderID, StudentID, DrinkID, OrderAmount) VALUES (@orderId, @studentId, @drinkId, @orderAmount)";

            // Create the SQL parameters
            SqlParameter[] parameters = new SqlParameter[]
            {
            new SqlParameter("@orderId", maxId + 1),
            new SqlParameter("@studentId", studentId),
            new SqlParameter("@drinkId", drinkId),
            new SqlParameter("@orderAmount", orderAmount)
            };

            // Call the ExecuteEditQuery method to execute the query
            ExecuteEditQuery(query, parameters);
        }
    }

}