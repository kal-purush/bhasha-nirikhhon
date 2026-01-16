using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Bangazon_RedJags.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace Bangazon_RedJags.Controllers
{
    public class TrainingProgramsController : Controller
    {

        private readonly IConfiguration _config;

        public TrainingProgramsController(IConfiguration config)
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

        // GET: TrainingPrograms
        public ActionResult Index()
        {
            using (SqlConnection conn = Connection)
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = @"SELECT Id, [Name], StartDate, EndDate, MaxAttendees 
                                        FROM TrainingProgram
                                        WHERE StartDate >= @today";

                    cmd.Parameters.Add(new SqlParameter("@today", DateTime.Now));
                    SqlDataReader reader = cmd.ExecuteReader();
                    List<TrainingProgram> trainingPrograms = new List<TrainingProgram>();

                    int IdOrdinal = reader.GetOrdinal("Id");
                    int NameOrdinal = reader.GetOrdinal("Name");
                    int StartDateOrdinal = reader.GetOrdinal("StartDate");
                    int EndDateOrdinal = reader.GetOrdinal("EndDate");
                    int MaxAttendeesOrdinal = reader.GetOrdinal("MaxAttendees");

                    while (reader.Read())
                    {
                        TrainingProgram trainingProgram = new TrainingProgram
                        {
                            Id = reader.GetInt32(IdOrdinal),
                            Name = reader.GetString(NameOrdinal),
                            StartDate = reader.GetDateTime(StartDateOrdinal),
                            EndDate = reader.GetDateTime(EndDateOrdinal),
                            MaxAttendees = reader.GetInt32(MaxAttendeesOrdinal)
                        };

                        trainingPrograms.Add(trainingProgram);
                    }
                    reader.Close();

                    return View(trainingPrograms);
                }
            }
        }

        // GET: TrainingPrograms/Details/5
        public ActionResult Details(int id)
        {
            return View();
        }

        // GET: TrainingPrograms/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: TrainingPrograms/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Create(TrainingProgram trainingProgram
            )
        {
            try
            {
                using (SqlConnection conn = Connection)
                {
                    conn.Open();
                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        if (trainingProgram.StartDate < DateTime.Now)
                        {
                            ViewBag.ErrorMessage1 = "STOP.  We can't go back to the future.  Please adjust start date accordingly";
                            return View();
                        }
                        else if (trainingProgram.StartDate > trainingProgram.EndDate)
                        {
                            ViewBag.ErrorMessage2 = "STOP.  Collaborate and listen.  The ending date must be after the session.";
                            return View();
                        }
                        else
                        {
                            cmd.CommandText = @"INSERT INTO TrainingProgram ([Name], StartDate, EndDate, MaxAttendees)
                                            OUTPUT INSERTED.id
                                            VALUES (@name, @startDate, @endDate, @maxAttendees)";

                            cmd.Parameters.Add(new SqlParameter("@name", trainingProgram.Name));
                            cmd.Parameters.Add(new SqlParameter("@startDate", trainingProgram.StartDate));
                            cmd.Parameters.Add(new SqlParameter("@endDate", trainingProgram.EndDate));
                            cmd.Parameters.Add(new SqlParameter("@MaxAttendees", trainingProgram.MaxAttendees));

                            int newId = (int)cmd.ExecuteScalar();
                            trainingProgram.Id = newId;

                            
                        }
                    }
                }

                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }

        // GET: TrainingPrograms/Edit/5
        public ActionResult Edit(int id)
        {
            return View();
        }

        // POST: TrainingPrograms/Edit/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Edit(int id, IFormCollection collection)
        {
            try
            {
                // TODO: Add update logic here

                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }

        // GET: TrainingPrograms/Delete/5
        public ActionResult Delete(int id)
        {
            return View();
        }

        // POST: TrainingPrograms/Delete/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Delete(int id, IFormCollection collection)
        {
            try
            {
                // TODO: Add delete logic here

                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }
    }
}