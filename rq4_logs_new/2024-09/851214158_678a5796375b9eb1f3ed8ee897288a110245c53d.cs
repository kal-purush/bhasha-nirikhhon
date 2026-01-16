using Microsoft.EntityFrameworkCore;
using ProjectMVC.BLL.Interfacies;
using ProjectMVC.DAL.Data;
using ProjectMVC.DAL.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProjectMVC.BLL.Repositories
{
    public class EmployeeRepository : IEmployeeRepository
    {
        private readonly AppDbContext _dbcontext;

        public EmployeeRepository(AppDbContext dbContext)
        {
            _dbcontext = dbContext;
        }

        public int Add(Employee Employee)
        {
            _dbcontext.Employees.Add(Employee);
            return _dbcontext.SaveChanges();
        }

        public int Delete(Employee Employee)
        {
            _dbcontext.Employees.Remove(Employee);
            return _dbcontext.SaveChanges();
        }

        public IEnumerable<Employee> GetAll()
        {
            return _dbcontext.Employees.AsNoTracking().ToList();
        }

        public Employee GetById(int id)
        {
            return _dbcontext.Employees.Find(id);
        }

        public int Update(Employee Employee)
        {
            _dbcontext.Employees.Update(Employee);
            return _dbcontext.SaveChanges();
        }
    }
}