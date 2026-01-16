using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dotnet_course_management.Data;
using dotnet_course_management.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace dotnet_course_management.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class BasicStudentController : ControllerBase
    {
        private readonly DataContext _context;
        public BasicStudentController(DataContext context)
        {
            _context = context;
            
        }

        [HttpGet("GetAllStudents")]
        public async Task<ActionResult<List<Student>>> GetStudents()
        {
            return Ok(await _context.Students.ToListAsync());
        }

        [HttpPost("CreateStudent")]
        public async Task<ActionResult<List<Student>>> CreateStudent(Student student)
        {
            _context.Students.Add(student);
            await _context.SaveChangesAsync();

            return Ok(await _context.Students.ToListAsync());
        }

        [HttpPut("UpdateStudent")]
        public async Task<ActionResult<List<Student>>> UpdateStudent(Student student)
        {
            var dbStudent = await _context.Students.FindAsync(student.Id);
            if(dbStudent == null)
            {
                return BadRequest("Student Not Found");
            }
            dbStudent.FirstName = student.FirstName;
            dbStudent.LastName = student.LastName;

            await _context.SaveChangesAsync();
            return Ok(await _context.Students.ToListAsync());
        }

        [HttpDelete("{id}")]
        public async Task<ActionResult<List<Student>>> DeleteStudent(int id)
        {
            var dbStudent = await _context.Students.FindAsync(id);
            if(dbStudent == null)
            {
                return BadRequest("Student Not Found");
            }

            _context.Students.Remove(dbStudent);
            await _context.SaveChangesAsync();

            return Ok(await _context.Students.ToListAsync());
        }
    }

}