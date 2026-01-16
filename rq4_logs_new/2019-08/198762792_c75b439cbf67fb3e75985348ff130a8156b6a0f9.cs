using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TicketingSystem.BLL.Helpers;
using TicketingSystem.DAL.Models;
using TicketingSystem.ViewModel.ViewModel;
using TicketingSystem.ViewModel.ViewModels;

namespace TicketingSystem.BLL.Contracts
{
    public class EmployeeService : IGenericService<EmployeeVM, long>
    {
        private ToViewModel toViewModel = new ToViewModel();
        private ToModel toModel = new ToModel();
        private readonly TicketingSystemContext context;

        //inject dependencies
        public EmployeeService(TicketingSystemContext _context) {
            context = _context;
        }

        public ResponseVM Create(EmployeeVM employeeVM) {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        //converts to book type then save to context
                        context.Employees.Add(toModel.Employee(employeeVM));
                        context.SaveChanges();

                        //commits changes to db
                        dbTransaction.Commit();
                        return new ResponseVM("created", true, "Employee");
                    }
                    catch (Exception ex)
                    {

                        dbTransaction.Rollback();
                        return new ResponseVM("created", false, "Employee", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
                    }
                }
            }
        }
        public ResponseVM Update(EmployeeVM employeeVM) {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        //find employee from the database
                        Employee employeeToBeUpdated = context.Employees.Find(employeeVM.EmployeeID);
                        //ends function if employee does not exists
                        if (employeeToBeUpdated == null)
                        {
                            return new ResponseVM("updated", false, "Employee", ResponseVM.DOES_NOT_EXIST);
                        }

                        //update changes then save to context
                        employeeToBeUpdated.EmployeeID = employeeVM.EmployeeID;
                        employeeToBeUpdated.FirstName = employeeVM.FirstName;
                        employeeToBeUpdated.LastName = employeeVM.LastName;
                        employeeToBeUpdated.EmailAddress = employeeVM.EmailAddress;
                        employeeToBeUpdated.Office = employeeVM.Office;
                        context.SaveChanges();

                        //commit changes to db
                        dbTransaction.Commit();
                        return new ResponseVM("updated", true, "Employee");
                    }
                    catch (Exception ex)
                    {
                        dbTransaction.Rollback();
                        return new ResponseVM("updated", false, "Employee", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
                    }
                }
            }
        }
        public ResponseVM Delete(long id)
        {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    
                    try
                    {
                        Employee employeeToBeDeleted = context.Employees.Find(id);
                        //ends function if employee does not exists
                        if (employeeToBeDeleted == null)
                        {
                            return new ResponseVM("deleted", false, "Employee", ResponseVM.DOES_NOT_EXIST);
                        }
                        //delete employee then save to context
                        context.Employees.Remove(employeeToBeDeleted);
                        context.SaveChanges();

                        //commit changes to db
                        dbTransaction.Commit();
                        return new ResponseVM("deleted", true, "Employee");
                    }
                    catch (Exception ex)
                    {
                        dbTransaction.Rollback();
                        return new ResponseVM("deleted", false, "Employee", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
                        
                    }
                }
            }
        }
        public IEnumerable<EmployeeVM> GetAll() {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        //gets all employees and order the from last to first
                        var employees = context.Employees.ToList().OrderByDescending(x => x.EmployeeID);
                        var employeesVm = employees.Select(x=>toViewModel.Employee(x));
                        return employeesVm;
                    }
                    catch (Exception ex)
                    {
                        throw;
                    }
                }
            }
        }
        public EmployeeVM GetSingleBy(long id)
        {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        var employee = context.Employees.Find(id);
                        EmployeeVM employeeVm = null;
                        if (employee != null)
                        {
                            employeeVm = toViewModel.Employee(employee);
                        }
                        return employeeVm;
                    }
                    catch (Exception)
                    {

                        throw;
                    }
                }
            }
        }
    }
}