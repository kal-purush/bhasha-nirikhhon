using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TicketingSystem.BLL.Contracts;
using TicketingSystem.BLL.Helpers;
using TicketingSystem.DAL.Models;
using TicketingSystem.ViewModel.ViewModel;
using TicketingSystem.ViewModel.ViewModels;

namespace TicketingSystem.BLL.Services
{
    public class EmployeeTypeService : IGenericService<EmployeeTypeVM>
    {
        private ToViewModel toViewModel = new ToViewModel();
        private ToModel toModel = new ToModel();
        private readonly TicketingSystemContext context;

        //inject dependencies
        public EmployeeTypeService(TicketingSystemContext _context)
        {
            context = _context;
        }

        public ResponseVM Create(EmployeeTypeVM employeetypeVM)
        {
            using (context)
            {
                //check if record already exists
                if (context.EmployeeTypes.Where(b => b.EmployeeTypeName.ToLower() == employeetypeVM.EmployeeTypeName.ToLower()).Any())
                {
                    return new ResponseVM("created", false, "Employee Type", ResponseVM.ALREADY_EXIST);
                }
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        employeetypeVM.EmployeeTypeid = Guid.NewGuid();
                        //converts to book type then save to context
                        context.EmployeeTypes.Add(toModel.EmployeeType(employeetypeVM));
                        context.SaveChanges();

                        //commits changes to db
                        dbTransaction.Commit();
                        return new ResponseVM("created", true, "Employee Type");
                    }
                    catch (Exception ex)
                    {

                        dbTransaction.Rollback();
                        return new ResponseVM("created", false, "Employee Type", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
                    }
                }
            }
        }
        public ResponseVM Update(EmployeeTypeVM employeetypeVM)
        {
            using (context)
            {
                //check if record already exists
                if (context.EmployeeTypes.Where(b => b.EmployeeTypeName.ToLower() == employeetypeVM.EmployeeTypeName.ToLower()).Any())
                {
                    return new ResponseVM("created", false, "Employee Type", ResponseVM.ALREADY_EXIST);
                }
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        //find employee from the database
                        EmployeeType employeeTypeToBeUpdated = context.EmployeeTypes.Find(employeetypeVM.EmployeeTypeid);
                        //ends function if employee does not exists
                        if (employeeTypeToBeUpdated == null)
                        {
                            return new ResponseVM("updated", false, "Employee Type", ResponseVM.DOES_NOT_EXIST);
                        }

                        //update changes then save to context
                        employeeTypeToBeUpdated.EmployeeTypeid = employeetypeVM.EmployeeTypeid;
                        employeeTypeToBeUpdated.EmployeeTypeName = employeetypeVM.EmployeeTypeName;
                        context.SaveChanges();

                        //commit changes to db
                        dbTransaction.Commit();
                        return new ResponseVM("updated", true, "Employee Type");
                    }
                    catch (Exception ex)
                    {
                        dbTransaction.Rollback();
                        return new ResponseVM("updated", false, "Employee Type", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
                    }
                }
            }
        }
        public ResponseVM Delete(Guid id)
        {
            using (context)
            {
                //check if employee exists in ticket
                if (context.Employees.Where(b => b.EmployeeTypeid == id).Any())
                {
                    return new ResponseVM("deleted", false, "Employee Type", "Can't delete record. It is used in a transaction");
                }
                using (var dbTransaction = context.Database.BeginTransaction())
                {

                    try
                    {
                        EmployeeType employeeTypeToBeDeleted = context.EmployeeTypes.Find(id);
                        //ends function if employee does not exists
                        if (employeeTypeToBeDeleted == null)
                        {
                            return new ResponseVM("deleted", false, "Employee Type", ResponseVM.DOES_NOT_EXIST);
                        }
                        //delete employee then save to context
                        context.EmployeeTypes.Remove(employeeTypeToBeDeleted);
                        context.SaveChanges();

                        //commit changes to db
                        dbTransaction.Commit();
                        return new ResponseVM("deleted", true, "Employee Type");
                    }
                    catch (Exception ex)
                    {
                        dbTransaction.Rollback();
                        return new ResponseVM("deleted", false, "Employee Type", ResponseVM.SOMETHING_WENT_WRONG, "", ex);

                    }
                }
            }
        }
        public IEnumerable<EmployeeTypeVM> GetAll()
        {
            using (context)
            {
                try
                {
                    //gets all employees and order the from last to first
                    var employeetypes = context.EmployeeTypes.ToList().OrderByDescending(x => x.EmployeeTypeid);
                    var employeetypesVm = employeetypes.Select(x => toViewModel.EmployeeType(x));
                    return employeetypesVm;
                }
                catch (Exception ex)
                {
                    throw;
                }
            }
        }
        public EmployeeTypeVM GetSingleBy(Guid id)
        {
            using (context)
            {
                try
                {
                    var employeetypes = context.EmployeeTypes.Find(id);
                    EmployeeTypeVM employeetypeVM = null;
                    if (employeetypes != null)
                        employeetypeVM = toViewModel.EmployeeType(employeetypes);
                    return employeetypeVM;
                }
                catch (Exception)
                {
                    throw;
                }
            }
        }
    }
}