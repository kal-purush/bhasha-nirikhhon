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
using static TicketingSystem.ViewModel.ViewModels.DatatableVM;

namespace TicketingSystem.BLL.Services
{
    public class ITGroupMemberService : IGenericService<ITGroupMemberVM>
    {
        private ToViewModel toViewModel = new ToViewModel();
        private ToModel toModel = new ToModel();
        private object itgroupmemberSaved;
        private readonly TicketingSystemContext context;


        public ITGroupMemberService(TicketingSystemContext _context)
        {
            context = _context;
        }

        public ResponseVM Create(ITGroupMemberVM itgroupmemberVM)
        {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        itgroupmemberVM.ITGroupMemberid = Guid.NewGuid().ToString();
                        var itgroupmemberSaved = context.ITGroupMembers.Add(toModel.ITGroupMember(itgroupmemberVM)).Entity;
                        context.SaveChanges();

                        foreach (var empID in itgroupmemberVM.EmployeeIdList)
                        {
                            
                            var employee = context.Employees.Find(empID);
                            if (employee == null)
                                return new ResponseVM("create", false, "ITGroupMember", "Employee does not exists");
                            var groupEmployee = new GroupEmployee
                            {
                                EmployeeID = empID,
                                ITGroupMemberid = itgroupmemberSaved.ITGroupMemberid,
                                EmployeeFullName = toViewModel.ToFullName(employee.FirstName,  employee.LastName)
                            };

                            context.GroupEmployees.Add(groupEmployee);
                            context.SaveChanges();
                        }


                        dbTransaction.Commit();
                        return new ResponseVM
                            ("create", true, "ITGroupMember");

                    }
                    catch (Exception ex)
                    {
                        dbTransaction.Rollback();
                        return new ResponseVM("create", false, "ITGroupMember", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
                    }
                }
            }
        }
        public ResponseVM Delete(Guid id)
        {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        ITGroupMember itgroupmemberTobeDeleted = context.ITGroupMembers.Find(id);
                        if (itgroupmemberTobeDeleted == null)
                            return new ResponseVM("deleted", false, "ITGroupMember", ResponseVM.DOES_NOT_EXIST);
                        //delete

                        var removeFromGroupEmployees = context.GroupEmployees.Where(x => x.ITGroupMemberid == itgroupmemberTobeDeleted.ITGroupMemberid);
                        context.GroupEmployees.RemoveRange(removeFromGroupEmployees);
                        context.SaveChanges();

                        context.ITGroupMembers.Remove(itgroupmemberTobeDeleted);
                        context.SaveChanges();

                        dbTransaction.Commit();
                        return new ResponseVM
                            ("deleted", true, "ITGroupMember");
                    }
                    catch (Exception ex)
                    {
                        dbTransaction.Rollback();
                        return new ResponseVM
                            ("deleted", false, "ITGroupMember", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
                    }
                }
            }
        }
        public IEnumerable<ITGroupMemberVM> GetAll()
        {
            using (context)
            {
                try
                {
                    var itgroupmembers = context.ITGroupMembers
                        .Include(x => x.ITGroup)
                        .Include(x => x.GroupEmployees)
                            .ThenInclude(x => x.Employee)
                        .ToList()
                        .OrderByDescending(x => x.ITGroupMemberid);
                    var itgroupmembersVm = itgroupmembers.Select(x => toViewModel.ITGroupMember(x));
                    return itgroupmembersVm;
                }
                catch (Exception)
                {

                    throw;
                }
            }
        }

        public ITGroupMemberVM GetSingleBy(Guid id)
        {
            using (context)
            {
                try
                {
                    
                    var itgroupmember = context.ITGroupMembers
                        .Include(x => x.ITGroup)
                        .Where(x => x.ITGroupMemberid == id)
                        .FirstOrDefault();
                    ITGroupMemberVM itgroupmemberVM = null;
                    if (itgroupmember != null)
                        itgroupmemberVM = toViewModel.ITGroupMember(itgroupmember);
                    return itgroupmemberVM;

                }
                catch (Exception)
                {

                    throw;
                }
            }
        }

        public ResponseVM Update(ITGroupMemberVM itgroupmemberVM)
        {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {

                        ITGroupMember itgroupmemberTobeUpdated = context.ITGroupMembers.Where(x => x.ITGroupMemberid == Guid.Parse( itgroupmemberVM.ITGroupMemberid)).FirstOrDefault();

                        if (itgroupmemberTobeUpdated == null)
                            return new ResponseVM("update", false, "ITGroupMember", ResponseVM.DOES_NOT_EXIST);
                    
                        itgroupmemberTobeUpdated.ITGroupid =Guid.Parse(itgroupmemberVM.ITGroupid);
                        context.SaveChanges();

                        var removeFromGroupEmployees = context.GroupEmployees.Where(x => x.ITGroupMemberid == itgroupmemberTobeUpdated.ITGroupMemberid);
                        context.GroupEmployees.RemoveRange(removeFromGroupEmployees);
                        context.SaveChanges();


                        foreach (var empID in itgroupmemberVM.EmployeeIdList)
                        {
                            var employee = context.Employees.Find(empID);
                            if (employee == null)
                                return new ResponseVM("create", false, "ITGroupMember", "Employee does not exists");
                            var groupEmployee = new GroupEmployee
                            {
                                EmployeeID = empID,
                                ITGroupMemberid = itgroupmemberTobeUpdated.ITGroupMemberid,
                                EmployeeFullName = toViewModel.ToFullName(employee.FirstName, employee.LastName)
                            };

                            context.GroupEmployees.Add(groupEmployee);
                            context.SaveChanges();
                        }

                        dbTransaction.Commit();
                        return new ResponseVM("updated", true, "ITGroupMember");
                    }
                    catch (Exception ex)
                    {
                        dbTransaction.Rollback();
                        return new ResponseVM
                            ("updated", false, "ITGroupMember", ResponseVM.SOMETHING_WENT_WRONG, "", ex);

                        throw;
                    }

                }
            }
        }

        public PagingResponse<ITGroupMemberVM> GetDataServerSide(PagingRequest paging)
        {
            using (context)
            {
                var pagingResponse = new PagingResponse<ITGroupMemberVM>()
                {
                    // counts how many times the user draws data
                    Draw = paging.Draw
                };
                // initialized query
                IEnumerable<ITGroupMember> query = null;
                // search if user provided a search value, i.e. search value is not empty
                if (!string.IsNullOrEmpty(paging.Search.Value))
                {
                    // search based from the search value     
                    query = context.ITGroupMembers.Include(x => x.ITGroup)
                                         .Include(x => x.GroupEmployees)
                                         .ThenInclude(x => x.Employee)
                                         .Where(v => 
                                                     v.ITGroup.ITGroupName.ToString().ToLower().Contains(paging.Search.Value.ToLower()) ||
                                                     v.GroupEmployees.Any(x => x.EmployeeFullName.ToLower().Contains(paging.Search.Value.ToLower())));
                }
                else
                {
                    // selects all from table
                    query = context.ITGroupMembers
                                        .Include(x => x.ITGroup)
                                        .Include(x => x.GroupEmployees)
                                        .ThenInclude(x => x.Employee);
                }
                // total records from query
                var recordsTotal = query.Count();
                // orders the data by the sorting selected by the user
                // used ternary operator to determine if ascending or descending
                var colOrder = paging.Order[0];
                switch (colOrder.Column)
                {
                    case 0:
                        query = colOrder.Dir == "asc" ? query.OrderBy(b => b.ITGroup.ITGroupName) : query.OrderByDescending(b => b.ITGroup.ITGroupName);
                        break;
                   
                }
                var taken = query.Skip(paging.Start).Take(paging.Length).ToArray();
                // converts model(query) into viewmodel then assigns it to response which is displayed as "data"
                pagingResponse.Reponse = taken.Select(x => toViewModel.ITGroupMember(x));
                pagingResponse.RecordsTotal = recordsTotal;
                pagingResponse.RecordsFiltered = recordsTotal;

                return pagingResponse;
            }
        }
    }
}
  



                
            
      
    