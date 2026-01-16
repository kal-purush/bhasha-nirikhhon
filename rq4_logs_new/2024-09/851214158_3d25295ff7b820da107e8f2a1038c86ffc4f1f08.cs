using ProjectMVC.BLL.Interfacies;
using ProjectMVC.DAL.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProjectMVC.BLL.Repositories
{
    public class UnitOfWork : IUnitOfWork 
    {
        private readonly AppDbContext _dbContext;

        public UnitOfWork(AppDbContext dbContext)
        {
            _dbContext = dbContext;
            EmployeeRepository = new EmployeeRepository(_dbContext);
            DepartementRepository = new DepartementRepository(_dbContext); 
        }
        public IEmployeeRepository EmployeeRepository { get; set ; }
        public IDepartementRepository DepartementRepository { get ; set ; }

        public int Complete()
        {
            return _dbContext.SaveChanges();
        }

        public void Dispose()
        {
            _dbContext.Dispose();
        }

    }
}