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
    public class DepartementRepository : IDepartementRepository
    {
        private readonly AppDbContext _dbcontext;

        public DepartementRepository(AppDbContext dbContext)
        {
            _dbcontext = dbContext;
        }

        public int Add(Department department)
        {
            _dbcontext.Departments.Add(department);
            return _dbcontext.SaveChanges();
        }

        public int Delete(Department department)
        {
            _dbcontext.Departments.Remove(department);
            return _dbcontext.SaveChanges();
        }

        public IEnumerable<Department> GetAll()
        {
            return _dbcontext.Departments.AsNoTracking().ToList();
        }

        public Department GetById(int id)
        {
            return _dbcontext.Departments.Find(id);
        }

        public int Update(Department department)
        {
           _dbcontext.Departments.Update(department);
            return _dbcontext.SaveChanges();
        }
    }
}