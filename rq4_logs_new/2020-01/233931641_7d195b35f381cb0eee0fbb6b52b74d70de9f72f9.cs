using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using GreenMonkeysMVC.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;

namespace GreenMonkeysMVC.Controllers
{
    public class ComputersController : Controller
    {
        private readonly IConfiguration _config;

        public ComputersController(IConfiguration config)
        {
            _config = config;
        }

        public SqlConnection Connection
        {
            get
            {
                return new SqlConnection(_config.GetConnectionString("DefaultConnection"));
            }
        }


        // GET: All Computers
        public ActionResult Index()
        {
            using (SqlConnection conn = Connection)
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = @"SELECT c.Id as ComputerId, c.PurchaseDate, 
                                        c.DecomissionDate, c.Make, c.Model
                                        FROM Computer c";

                    var reader = cmd.ExecuteReader();

                    var computers = new List<Computer>();

                    while (reader.Read())
                    {
                        computers.Add(new Computer
                        {
                            Id = reader.GetInt32(reader.GetOrdinal("ComputerId")),
                            Make = reader.GetString(reader.GetOrdinal("Make")),
                            Model = reader.GetString(reader.GetOrdinal("Model")),
                            PurchaseDate = reader.GetDateTime(reader.GetOrdinal("PurchaseDate")),
                            // The code below checks to see if DecommissionDate is Null. If it is Null, it returns DateTime.MinValue.
                            DecomissionDate = reader.IsDBNull(reader.GetOrdinal("DecomissionDate")) ? DateTime.MinValue : reader.GetDateTime(reader.GetOrdinal("DecomissionDate"))
                        });

                    }
                    reader.Close();
                    return View(computers);

                }
            }

        }

        // GET: Computers/Details/5
        public ActionResult Details(int id)
        {
            using (SqlConnection conn = Connection)
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = @"SELECT c.Id as ComputerId, c.PurchaseDate, 
                                        c.DecomissionDate, c.Make, c.Model
                                        FROM Computer c
                                        WHERE Id = @id";
                    cmd.Parameters.Add(new SqlParameter("@Id", id));
                    var reader = cmd.ExecuteReader();

                    if (reader.Read())
                    {
                        var computer = new Computer
                        {
                            Id = reader.GetInt32(reader.GetOrdinal("ComputerId")),
                            Make = reader.GetString(reader.GetOrdinal("Make")),
                            Model = reader.GetString(reader.GetOrdinal("Model")),
                            PurchaseDate = reader.GetDateTime(reader.GetOrdinal("PurchaseDate")),
                            // The code below checks to see if DecommissionDate is Null. If it is Null, it returns DateTime.MinValue.
                            DecomissionDate = reader.IsDBNull(reader.GetOrdinal("DecomissionDate")) ? DateTime.MinValue : reader.GetDateTime(reader.GetOrdinal("DecomissionDate"))
                        };

                        reader.Close();
                        return View(computer);
                    }
                    reader.Close();
                    return NotFound();
                }
            }
        }



        // GET: Computers/Create
        public ActionResult Create()
        {
            return View();
        }


        // POST: Exercises/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Create(Computer computer)
        {
            try
            {
                using (SqlConnection conn = Connection)
                {
                    conn.Open();
                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = @"INSERT INTO Computer (Make, Model, PurchaseDate)
                                            VALUES (@Make, @Model, @PurchaseDate)";

                        cmd.Parameters.Add(new SqlParameter("@Make", computer.Make));
                        cmd.Parameters.Add(new SqlParameter("@Model", computer.Model));
                        cmd.Parameters.Add(new SqlParameter("@PurchaseDate", computer.PurchaseDate));

                        cmd.ExecuteNonQuery();
                    }
                }

                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }



        // GET: Computers/Delete/5
        public ActionResult Delete(int id)
        {
            using (SqlConnection conn = Connection)
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = @"SELECT Id, Make, Model, PurchaseDate, DecomissionDate
                                        FROM Computer
                                        WHERE Id = @Id";

                    cmd.Parameters.Add(new SqlParameter("@id", id));

                    var reader = cmd.ExecuteReader();

                    if (reader.Read())
                    {
                        var computer = new Computer
                        {
                            Id = reader.GetInt32(reader.GetOrdinal("Id")),
                            Make = reader.GetString(reader.GetOrdinal("Make")),
                            Model = reader.GetString(reader.GetOrdinal("Model")),
                            PurchaseDate = reader.GetDateTime(reader.GetOrdinal("PurchaseDate")),
                            // The code below checks to see if DecommissionDate is Null. If it is Null, it returns DateTime.MinValue.
                            DecomissionDate = reader.IsDBNull(reader.GetOrdinal("DecomissionDate")) ? DateTime.MinValue : reader.GetDateTime(reader.GetOrdinal("DecomissionDate"))
                        };

                        reader.Close();
                        return View(computer);
                    }
                    return NotFound();
                }
            }
        }

        // POST: Computers/Delete/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Delete(int id, Computer computer)
        {
            try
            {
                using (SqlConnection conn = Connection)
                {
                    conn.Open();
                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = @"SELECT c.Id, c.Make, c.Model, c.PurchaseDate, c.DecomissionDate
                                            FROM Computer c
                                            LEFT JOIN Employee e ON c.Id = e.ComputerId
                                            WHERE e.ComputerId Is Null AND  c.Id = @Id";

                        cmd.Parameters.Add(new SqlParameter("@id", id));
                        var reader = cmd.ExecuteReader();
                        if (reader.Read())
                        {
                            var compId = reader.GetInt32(reader.GetOrdinal("Id"));
                            if (id == compId)
                            {
                                reader.Close();
                                cmd.CommandText = @"DELETE FROM Computer WHERE Id = @Id";
                                cmd.ExecuteNonQuery();
                                return RedirectToAction(nameof(Index));

                            }
                            else
                            {
                                throw new Exception("This computer is still in use");
                            }
                        }
                        return RedirectToAction(nameof(Index));

                    }
                }

            }
            catch
            {
                return View();
            }
        }
    }
}