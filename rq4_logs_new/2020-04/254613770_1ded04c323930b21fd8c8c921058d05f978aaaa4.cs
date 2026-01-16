using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Tutorial_6.Models;

namespace Tutorial_6
{
    [Route("api/students")]
    [ApiController]
    public class StudentsController : ControllerBase
    {
        [HttpGet]
        public IActionResult GetStudents()
        {
            var list = new List<Student>();
            list.Add(new Student
            {
                IdStudent = 1,
                FirstName = "Jan",
                LastName = "Kowalski"
            });
            list.Add(new Student
            {
                IdStudent = 2,
                FirstName = "Andrzej",
                LastName = "Malewicz"
            });


            return Ok(list);
        }

        [HttpGet("{index}")]
        public IActionResult GetStudent(string index)
        {
            return Ok(new Student { FirstName = "Andrzej", LastName = "Malewicz" });
        }

    }
}