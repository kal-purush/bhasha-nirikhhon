using FYP_Management_System.DAL;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.EntityFrameworkCore;
using Microsoft.Identity.Client;
using NexGen.Models;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace FYP_Management_System.Pages
{
    public class StudentHomePageModel : PageModel
    {
        private readonly AppDbContext _context;
        private readonly ILogger<StudentHomePageModel> _logger;

        [BindProperty]
        public string Email { get; set; }
        public int Batch { get; set; }
        public string Department { get; set; }
        public string FYP_Name { get; set; }
        public float CGPA { get; set; }
        public string Name { get; set; }
        public int SupervisorID { get; set; }
        public string Details { get; set; }
        public string Domain { get; set; }

        public StudentHomePageModel(AppDbContext context, ILogger<StudentHomePageModel> logger)
        {
            _context = context;
            _logger = logger;
        }

        public void OnGet()
        {
            // Retrieve student information from your database or authentication service
            var student = _context.Students.FirstOrDefault();
            if (student != null)
            {
                Email = student.Email;
                Batch = student.Batch;
                Department = student.Department;
                FYP_Name = student.FYP_Name;
                CGPA = student.CGPA;
            }

            var FYP = _context.FYPs.FirstOrDefault();
            if (FYP != null)
            {
                Name = FYP.Name;
                SupervisorID = FYP.SupervisorId;
                Details = FYP.Details;
                Domain = FYP.Domain;
            }
        }

        public IActionResult OnPost(string title, string description)
        {
            // Retrieve the current user's email
            var userEmail = User.Identity.Name;

            // Retrieve the student information from your database or authentication service based on the user's email
            var student = _context.Students.FirstOrDefault(s => s.Email == userEmail);
            if (student != null)
            {
                // Access the student's properties
                Email = student.Email;
                Batch = student.Batch;
                Department = student.Department;
                FYP_Name = student.FYP_Name;
                CGPA = student.CGPA;
            }

            // Save the proposal to the database
            var proposal = new FYP
            {
                Name = title,
                Details = description
            };
            _context.FYPs.Add(proposal);
            _context.SaveChanges();

            // Redirect to a confirmation page or refresh the current page
            return RedirectToPage("StudentHomePage");
        }

    }
}
