using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using PunkHouseReal.Models;
using PunkHouseReal.Models.BindingModels;
using PunkHouseReal.Services;

namespace PunkHouseReal.Controllers.Api
{
    [Produces("application/json")]
    [Route("api/Expense")]
    public class ExpenseApiController : Controller
    {
        private readonly UserManager<ApplicationUser> _userManager;
        private readonly IExpenseService _expenseDataAccess;

        public ExpenseApiController(UserManager<ApplicationUser> userManager, IExpenseService expenseDataAccess)
        {
            _userManager = userManager;
            _expenseDataAccess = expenseDataAccess;
        }

        public IActionResult CreateExpense([FromBody]ExpenseBindingModel model)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    Expense expense = new Expense();
                    ParseExpenseFields(expense, model);
                    _expenseDataAccess.AddExpense(expense);
                    return Ok(expense);

                }
                catch (Exception)
                {

                    throw;
                }
            }

            return BadRequest("error creating the expense");
        }

        private void ParseExpenseFields(Expense expense, ExpenseBindingModel model)
        {
            expense.CreatorId = model.CreatorId;
            expense.Description = model.Description;
            expense.ExpenseType = model.ExpenseType;
            expense.Name = model.Name;
            expense.HouseId = model.HouseId;
        }
    }

}