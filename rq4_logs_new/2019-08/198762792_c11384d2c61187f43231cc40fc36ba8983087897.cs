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
    public class TicketMinorService : IGenericService<TicketMinorVM>
    {
        private ToViewModel toViewModel = new ToViewModel();
        private ToModel toModel = new ToModel();
        private readonly TicketingSystemContext context;

        public TicketMinorService(TicketingSystemContext _context)
        {
            context = _context;
        }
        public ResponseVM Create(TicketMinorVM ticketMinorVM)
        {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        ticketMinorVM.TicketMinorid = Guid.NewGuid();
                        context.TicketMinors.Add(toModel.TicketMinor(ticketMinorVM));
                        context.SaveChanges();

                        dbTransaction.Commit();
                        return new ResponseVM("created", true, "TicketMinor");
                    }
                    catch (Exception ex)
                    {
                        dbTransaction.Rollback();
                        return new ResponseVM("created", false, "TicketMinor", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
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
                        TicketMinor ticketMinorToBeDeleted = context.TicketMinors.Find(id);
                        if (ticketMinorToBeDeleted == null)
                            return new ResponseVM("deleted", false, "TicketMinor", ResponseVM.DOES_NOT_EXIST);

                        context.TicketMinors.Remove(ticketMinorToBeDeleted);
                        context.SaveChanges();

                        dbTransaction.Commit();
                        return new ResponseVM("deleted", true, "TicketMinor");
                    }
                    catch (Exception ex)
                    {
                        dbTransaction.Rollback();
                        return new ResponseVM("deleted", false, "TicketMinor", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
                    }
                }
            }
        }
        public IEnumerable<TicketMinorVM> GetAll()
        {
            using (context)
            {
                try
                {
                    var categories = context.TicketMinors
                        .Include(x => x.Office)
                        .Include(x => x.Employee)
                        .Include(x => x.CategoryList)
                        .ToList().OrderByDescending(x => x.TicketMinorid);
                    var categoriesVm = categories.Select(x => toViewModel.TicketMinor(x));
                    return categoriesVm;
                }
                catch (Exception)
                {
                    throw;
                }
            }
        }

        public TicketMinorVM GetSingleBy(Guid id)
        {
            using (context)
            {
                try
                {
                    var categories = context.TicketMinors.Find(id);
                    TicketMinorVM ticketMinorVM = null;
                    if (categories != null)
                        ticketMinorVM = toViewModel.TicketMinor(categories);
                    return ticketMinorVM;
                }
                catch (Exception)
                {
                    throw;
                }

            }
        }
        public ResponseVM Update(TicketMinorVM ticketMinorVM)
        {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        TicketMinor ticketMinorToBeUpdated = context.TicketMinors.Find(ticketMinorVM.TicketMinorid);
                        if (ticketMinorToBeUpdated == null)
                            return new ResponseVM("updated", false, "TicketMinor", ResponseVM.DOES_NOT_EXIST);
                        context.SaveChanges();

                        dbTransaction.Commit();
                        return new ResponseVM("updated", true, "TicketMinor");
                    }
                    catch (Exception ex)
                    {
                        dbTransaction.Rollback();
                        return new ResponseVM("updated", false, "TicketMinor", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
                    }
                }
            }
        }
    }
}
