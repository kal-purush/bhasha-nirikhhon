using Microsoft.AspNetCore.Mvc;
using MonezyAPI.Models;

namespace MonezyAPI.Interfaces
{
    public interface IExpensesRepository
    {
        public ActionResult<List<Expenses>> GetExpenses();

        public Task<ActionResult<Expenses>> GetExpensesById(int IdExpenses);

        public Task<IEnumerable<Expenses>> GetExpensesByUser(int UserId);

        public Task<ActionResult<Expenses>> InsertExpense(Expenses expense);

        public Task<Expenses?> PatchExpense(Expenses expense);

        public Task DeleteExpense(int IdExpense);

    }
}