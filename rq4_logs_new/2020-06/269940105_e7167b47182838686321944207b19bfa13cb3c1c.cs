using OrganisationArchive.DAL.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace OrganisationArchive.DAL.Repository
{
    public class EmployeeRepository : RepositoryBase<Employee>, IEmployeeRepository
    {
       
        public EmployeeRepository(OrganizationDbContext context)
            : base(context)
        { }

        public IEnumerable<Employee> GetAllEmplyee()
        {
            throw new NotImplementedException();
        }

        public Employee GetEmployeeById(int Id)
        {
            throw new NotImplementedException();
        }
    }
}