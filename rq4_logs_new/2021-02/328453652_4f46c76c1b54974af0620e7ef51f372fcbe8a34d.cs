using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Configuration;

namespace ADO.Practice
{
    public class DbHelper
    {
        private static string connectionStr = ConfigurationManager.ConnectionStrings["NorthwindConnection"].ConnectionString;

        private static string query1 = @"SELECT EmployeeID, FirstName, LastName FROM Northwind.dbo.Employees WHERE City = 'London'";

        private static string query2 = @"SELECT TOP(1) EmployeeID, Count(Distinct CustomerId) as Customers_Count  
                                         FROM Northwind.dbo.Orders ords
                                         GROUP BY EmployeeID
                                         ORDER BY Count(OrderID) desc";

        private static string query3 = @"SELECT ords.ShipCity, ords.ShipCountry, Count(*) as Orders_count  FROM Northwind.dbo.Orders ords
                                         GROUP BY ords.ShipCity, ords.ShipCountry
                                         HAVING Count(*) > 2";

        private static string query4 = @"SELECT TOP(1) ProductName, UnitPrice
                                         FROM Northwind.dbo.Products pr JOIN Northwind.dbo.Categories cts ON pr.CategoryID = cts.CategoryID
                                         WHERE cts.CategoryName = 'Seafood'
                                         ORDER BY UnitPrice desc";

        private static string query5 = @"SELECT DISTINCT csts.CustomerID, csts.ContactName 
                                         FROM Northwind.dbo.Customers csts JOIN Northwind.dbo.Orders ords ON csts.CustomerID = ords.CustomerID
                                         WHERE csts.City != ords.ShipCity";

        private static string query6 = @"SELECT DISTINCT t1.CustomerID, t1.ContactName 
                                         FROM
	                                         getIndexedCustomersOrdersJoin() t1 JOIN
                                             getIndexedCustomersOrdersJoin() t2
	                                         ON t1.CustomerID = t2.CustomerID and t1.ix = t2.ix + 1
                                         WHERE DATEDIFF(d, t2.OrderDate, t1.OrderDate) >= 183";

        private static string insertRoomQuery = @"INSERT INTO Northwind.dbo.Rooms(Id, Name) Values(@id, @name)";
        private static string insertSubjectQuery = @"INSERT INTO Northwind.dbo.Subjects(Id, Name, Description) Values (@id, @name, @description)";
        private static string insertLessonQuery = @"INSERT INTO Northwind.dbo.Lessons(Id, Subject_Fid, Room_Fid, Date) Values (@id, @subjectFid, @roomFid, @date)";

        private static string updateRoomQuery = @"UPDATE Northwind.dbo.Rooms SET Name = @name WHERE Id = @id";
        private static string updateSubjectQuery = @"UPDATE Northwind.dbo.Subjects SET Name = @name, Description = @description WHERE Id = @id";
        private static string updateLessonQuery = @"UPDATE Northwind.dbo.Lessons SET Date = @date, Subject_Fid = @subjectFid, Room_Fid = @roomFid WHERE Id = @id";

        private static string deleteRoomQuery = @"DELETE FROM Northwind.dbo.Lessons WHERE Id = @id";
        private static string deleteSubjectQuery = @"DELETE FROM Northwind.dbo.Rooms WHERE Id = @id";
        private static string deleteLessonQuery = @"DELETE FROM Northwind.dbo.Subjects WHERE Id = @id";

        public IEnumerable<string> GetQuery1()
        {
            var list = new List<string>();
            using (var connection = new SqlConnection(connectionStr))
            {
                connection.Open();
                var command = new SqlCommand(query1, connection);
                var reader = command.ExecuteReader();

                if (reader.HasRows)
                {
                    list.Add(string.Format("{0,20}{1,20}{2,20}", reader.GetName(0), reader.GetName(1), reader.GetName(2)));
                    list.Add(string.Empty);

                    while (reader.Read())
                    {
                        list.Add(string.Format("{0,20}{1,20}{2,20}", reader.GetInt32(0), reader.GetString(1), reader.GetString(2)));
                    }
                }
            }
            return list;
        }

        public IEnumerable<string> GetQuery2()
        {
            var list = new List<string>();
            using (var connection = new SqlConnection(connectionStr))
            {
                connection.Open();
                var command = new SqlCommand(query2, connection);
                var reader = command.ExecuteReader();

                if (reader.HasRows)
                {
                    list.Add(string.Format("{0,20}{1,20}", reader.GetName(0), reader.GetName(1)));
                    list.Add(string.Empty);

                    while (reader.Read())
                    {
                        list.Add(string.Format("{0,20}{1,20}", reader.GetInt32(0), reader.GetInt32(1)));
                    }
                }
            }
            return list;
        }

        public IEnumerable<string> GetQuery3()
        {
            var list = new List<string>();
            using (var connection = new SqlConnection(connectionStr))
            {
                connection.Open();
                var command = new SqlCommand(query3, connection);
                var reader = command.ExecuteReader();

                if (reader.HasRows)
                {
                    list.Add(string.Format("{0,20}{1,20}{2,20}", reader.GetName(0), reader.GetName(1), reader.GetName(2)));
                    list.Add(string.Empty);

                    while (reader.Read())
                    {
                        list.Add(string.Format("{0,20}{1,20}{2,20}", reader.GetString(0), reader.GetString(1), reader.GetInt32(2)));
                    }
                }
            }
            return list;
        }

        public IEnumerable<string> GetQuery4()
        {
            var list = new List<string>();
            using (var connection = new SqlConnection(connectionStr))
            {
                connection.Open();
                var command = new SqlCommand(query4, connection);
                var reader = command.ExecuteReader();

                if (reader.HasRows)
                {
                    list.Add(string.Format("{0,20}{1,20}", reader.GetName(0), reader.GetName(1)));
                    list.Add(string.Empty);

                    while (reader.Read())
                    {
                        list.Add(string.Format("{0,20}{1,20}", reader.GetString(0), reader.GetDecimal(1)));
                    }
                }
            }
            return list;
        }

        public IEnumerable<string> GetQuery5()
        {
            var list = new List<string>();
            using (var connection = new SqlConnection(connectionStr))
            {
                connection.Open();
                var command = new SqlCommand(query5, connection);
                var reader = command.ExecuteReader();

                if (reader.HasRows)
                {
                    list.Add(string.Format("{0,15}{1,25}", reader.GetName(0), reader.GetName(1)));
                    list.Add(string.Empty);

                    while (reader.Read())
                    {
                        list.Add(string.Format("{0,15}{1,25}", reader.GetString(0), reader.GetString(1)));
                    }
                }
            }
            return list;
        }

        public IEnumerable<string> GetQuery6()
        {
            var list = new List<string>();
            using (var connection = new SqlConnection(connectionStr))
            {
                connection.Open();
                var command = new SqlCommand(query6, connection);
                var reader = command.ExecuteReader();

                if (reader.HasRows)
                {
                    list.Add(string.Format("{0,15}{1,25}", reader.GetName(0), reader.GetName(1)));
                    list.Add(string.Empty);

                    while (reader.Read())
                    {
                        list.Add(string.Format("{0,15}{1,25}", reader.GetString(0), reader.GetString(1)));
                    }
                }
            }
            return list;
        }

        public void InsertRoom(int id, string name)
        {
            using (var connection = new SqlConnection(connectionStr))
            {
                connection.Open();
                var command = new SqlCommand(insertRoomQuery, connection);
                command.Parameters.AddWithValue("@id", id);
                command.Parameters.AddWithValue("@name", name);
                command.ExecuteNonQuery();
            }
        }

        public void InsertSubject(int id, string name, string description)
        {
            using (var connection = new SqlConnection(connectionStr))
            {
                connection.Open();
                var command = new SqlCommand(insertSubjectQuery, connection);
                command.Parameters.AddWithValue("@id", id);
                command.Parameters.AddWithValue("@name", name);
                command.Parameters.AddWithValue("@description", description);
                command.ExecuteNonQuery();
            }
        }

        public void InsertLesson(int id, int subjectId, int roomId, DateTime date)
        {
            using (var connection = new SqlConnection(connectionStr))
            {
                connection.Open();
                var command = new SqlCommand(insertLessonQuery, connection);
                command.Parameters.AddWithValue("@id", id);
                command.Parameters.AddWithValue("@subjectFid", subjectId);
                command.Parameters.AddWithValue("@roomFid", roomId);
                command.Parameters.AddWithValue("@date", date);
                command.ExecuteNonQuery();
            }
        }

        public void UpdateRoom(int id, string name)
        {
            using (var connection = new SqlConnection(connectionStr))
            {
                connection.Open();
                var command = new SqlCommand(updateRoomQuery, connection);
                command.Parameters.AddWithValue("@id", id);
                command.Parameters.AddWithValue("@name", name);
                command.ExecuteNonQuery();
            }
        }

        public void UpdateSubject(int id, string name, string description)
        {
            using (var connection = new SqlConnection(connectionStr))
            {
                connection.Open();
                var command = new SqlCommand(updateSubjectQuery, connection);
                command.Parameters.AddWithValue("@id", id);
                command.Parameters.AddWithValue("@name", name);
                command.Parameters.AddWithValue("@description", description);
                command.ExecuteNonQuery();
            }
        }

        public void UpdateLesson(int id, int subjectId, int roomId, DateTime date)
        {
            using (var connection = new SqlConnection(connectionStr))
            {
                connection.Open();
                var command = new SqlCommand(updateLessonQuery, connection);
                command.Parameters.AddWithValue("@id", id);
                command.Parameters.AddWithValue("@subjectFid", subjectId);
                command.Parameters.AddWithValue("@roomFid", roomId);
                command.Parameters.AddWithValue("@date", date);
                command.ExecuteNonQuery();
            }
        }

        public void DeleteRoom(int id)
        {
            using (var connection = new SqlConnection(connectionStr))
            {
                connection.Open();
                var command = new SqlCommand(deleteRoomQuery, connection);
                command.Parameters.AddWithValue("@id", id);
                command.ExecuteNonQuery();
            }
        }

        public void DeleteSubject(int id)
        {
            using (var connection = new SqlConnection(connectionStr))
            {
                connection.Open();
                var command = new SqlCommand(deleteSubjectQuery, connection);
                command.Parameters.AddWithValue("@id", id);
                command.ExecuteNonQuery();
            }
        }

        public void DeleteLesson(int id)
        {
            using (var connection = new SqlConnection(connectionStr))
            {
                connection.Open();
                var command = new SqlCommand(deleteLessonQuery, connection);
                command.Parameters.AddWithValue("@id", id);
                command.ExecuteNonQuery();
            }
        }
    }
}