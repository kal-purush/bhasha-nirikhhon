using Microsoft.EntityFrameworkCore;
using studentprojectapi.GeneratedModels;
using studentprojectapi.Models;

namespace studentprojectapi.Services
{
    public class AssignmentServices
    {

        private readonly studentprojectContext _database_context;


        public AssignmentServices(studentprojectContext studenttablecontext) { _database_context = studenttablecontext; }


        // get users assigned to departments from assignments table
        public async Task<List<assignment>> GetAssignmentsAsync()
        {
            List<assignment> ListofAssignments = await (from row in _database_context.assignments select row).ToListAsync();

            return ListofAssignments;   
        }

        // write function to add people to departments
        public async Task AddAssignmentsAsync(AssignmentDTO assignment)
        {
            assignment assignmentobject = new assignment();

            // map assignment DTO from generated model
            assignmentobject.assignmentID = Guid.NewGuid();
            assignmentobject.createdby = assignment.CreatedBy;
            assignmentobject.createdate = DateTime.Now;
            assignmentobject.modifiedby = assignment.ModifiedBy;
            assignmentobject.modifieddate = DateTime.Now;
            

            // we would need to have a existing employee ID
            // and department ID passed in from the body of the get request
            // need to define it earlier? in order to have it sent in body
            // also need to make sure that the employee and deparment is available to map to?
            // got to find examples

        }



        // write function to remove people from departments

        // do I need a modify?


    }
}