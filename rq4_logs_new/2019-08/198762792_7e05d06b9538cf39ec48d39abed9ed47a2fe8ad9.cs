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
    public class OfficeService : IGenericService<OfficeVM, long>
    {
        private ToViewModel toViewModel = new ToViewModel();
        private ToModel toModel = new ToModel();
        private readonly TicketingSystemContext context;

        public OfficeService(TicketingSystemContext _context)
        {
            context = _context;
        }
        public ResponseVM Create(OfficeVM officeVM)
        {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        
                        context.Offices.Add(toModel.Office(officeVM));
                        context.SaveChanges();

                        dbTransaction.Commit();
                        return new ResponseVM("created", true, "Office");
                    }
                    catch (Exception ex)
                    {
                        dbTransaction.Rollback();
                        return new ResponseVM("created", false, "Office", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
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
                        Office officeToBeDeleted = context.Offices.Find(id);
                        if (officeToBeDeleted == null)
                            return new ResponseVM("deleted", false, "Office", ResponseVM.DOES_NOT_EXIST);

                        context.Offices.Remove(officeToBeDeleted);
                        context.SaveChanges();

                        dbTransaction.Commit();
                        return new ResponseVM("deleted", true, "Office");
                    }
                    catch (Exception ex)
                    {
                        dbTransaction.Rollback();
                        return new ResponseVM("deleted", false, "Office", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
                    }
                }
            }
        }
        public IEnumerable<OfficeVM> GetAll()
        {
            using (context)
            {
                try
                {
                    var offices = context.Offices.ToList().OrderByDescending(x => x.Officeid);
                    var officesVm = offices.Select(x => toViewModel.Office(x));
                    return officesVm;
                }
                catch (Exception)
                {
                    throw;
                }
            }
        }

        public OfficeVM GetSingleBy(long id)
        {
            using (context)
            {
                try
                {
                    var offices = context.Offices.Find(id);
                    OfficeVM officeVM = null;
                    if (offices != null)
                        officeVM = toViewModel.Office(offices);
                    return officeVM;
                }
                catch (Exception)
                {
                    throw;
                }

            }
        }
        public ResponseVM Update(OfficeVM officeVM)
        {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        Office officeToBeUpdated = context.Offices.Find(officeVM.Officeid);
                        if (officeToBeUpdated == null)
                            return new ResponseVM("updated", false, "Office", ResponseVM.DOES_NOT_EXIST);

                        officeToBeUpdated.OfficeCode = officeVM.OfficeCode;
                        officeToBeUpdated.OfficeDesc = officeVM.OfficeDesc;
                        context.SaveChanges();

                        dbTransaction.Commit();
                        return new ResponseVM("updated", true, "Office");
                    }
                    catch (Exception ex)
                    {
                        dbTransaction.Rollback();
                        return new ResponseVM("updated", false, "Office", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
                    }
                }
            }
        }
    }
}
