using System.Linq;
using ExpenseTracker.Repository.Entities;
using ExpenseTracker.Repository.Interfaces;

namespace ExpenseTracker.Repository
{
    public class ExpenseGroupStatusRepository : IExpenseTrackerGenericRepository<ExpenseGroupStatus>
    {
        private readonly IExpenseTrackerDbContext _context;

        public ExpenseGroupStatusRepository(IExpenseTrackerDbContext context)
        {
            _context = context;
        }

        public ExpenseGroupStatus GetById(int id)
        {
            return _context.ExpenseGroupStatusses.FirstOrDefault(egs => egs.Id == id);
        }

        public IQueryable<ExpenseGroupStatus> GetAllAsQueryable()
        {
            return _context.ExpenseGroupStatusses;
        }
    }
}