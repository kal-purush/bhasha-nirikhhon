using HRM.ApplicationCore.Contract.Service;
using HRM.ApplicationCore.Model.Request;
using Microsoft.AspNetCore.Mvc;

namespace HRM.WebMVCApp.Controllers
{
    public class EmployeeRoleController : Controller
    {
        private readonly IEmployeeRoleServiceAsync employeeRoleServiceAsync;

        public EmployeeRoleController(IEmployeeRoleServiceAsync _employeeRoleServiceAsync)
        {
            employeeRoleServiceAsync = _employeeRoleServiceAsync;
        }
        public async Task<IActionResult> Index()
        {
            var employeeRoleCollection = await employeeRoleServiceAsync.GetAllAsync();
            return View(employeeRoleCollection);
        }

        public IActionResult Create()

        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> Create(EmployeeRoleRequestModel model)
        {
            if (ModelState.IsValid)
            {
                // add to db
                await employeeRoleServiceAsync.AddAsync(model);
                return RedirectToAction("Index");
            }
            return View(model);
        }
        public async Task<IActionResult> Edit(int id)
        {
            var result = await employeeRoleServiceAsync.GetByIdAsync(id);
            return View(result);
        }
        [HttpPost]
        public async Task<IActionResult> Edit(EmployeeRoleRequestModel model)
        {
            try
            {
                await employeeRoleServiceAsync.UpdateAsync(model);
                return RedirectToAction("Index");
            }
            catch (Exception ex)
            {
                return View(model);
            }
        }

        public async Task<IActionResult> Delete(int id)
        {
            var result = await employeeRoleServiceAsync.GetByIdAsync(id);
            return View(result);
        }

        [HttpPost]
        public async Task<IActionResult> Delete(EmployeeRoleRequestModel model)
        {
            await employeeRoleServiceAsync.DeleteAsync(model.Id);
            return RedirectToAction("Index");

        }
    }
}
