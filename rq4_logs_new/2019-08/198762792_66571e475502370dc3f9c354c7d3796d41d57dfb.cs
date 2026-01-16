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
    public class ITGroupService : IGenericService<ITGroupVM>
    {
        private ToViewModel toViewModel = new ToViewModel();
        private ToModel toModel = new ToModel();
        private readonly TicketingSystemContext context;

        public ITGroupService(TicketingSystemContext _context)
        {
            context = _context;
        }
        public ResponseVM Create(ITGroupVM itgroupVM)
        {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {

                        context.ITGroups.Add(toModel.ITGroup(itgroupVM));
                        context.SaveChanges();

                        dbTransaction.Commit();
                        return new ResponseVM("created", true, "ITGroup");
                    }
                    catch (Exception ex)
                    {
                        dbTransaction.Rollback();
                        return new ResponseVM("created", false, "ITGroup", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
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
                        ITGroup itgroupToBeDeleted = context.ITGroups.Find(id);
                        if (itgroupToBeDeleted == null)
                            return new ResponseVM("deleted", false, "ITGroup", ResponseVM.DOES_NOT_EXIST);

                        context.ITGroups.Remove(itgroupToBeDeleted);
                        context.SaveChanges();

                        dbTransaction.Commit();
                        return new ResponseVM("deleted", true, "ITGroup");
                    }
                    catch (Exception ex)
                    {
                        dbTransaction.Rollback();
                        return new ResponseVM("deleted", false, "ITGroup", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
                    }
                }
            }
        }
        public IEnumerable<ITGroupVM> GetAll()
        {
            using (context)
            {
                try
                {
                    var itgroups = context.ITGroups.ToList().OrderByDescending(x => x.ITGroupid);
                    var itgroupsVm = itgroups.Select(x => toViewModel.ITGroup(x));
                    return itgroupsVm;
                }
                catch (Exception)
                {
                    throw;
                }
            }
        }

        public ITGroupVM GetSingleBy(Guid id)
        {
            using (context)
            {
                try
                {
                    var itgroups = context.ITGroups.Find(id);
                    ITGroupVM itgroupVM = null;
                    if (itgroups != null)
                        itgroupVM = toViewModel.ITGroup(itgroups);
                    return itgroupVM;
                }
                catch (Exception)
                {
                    throw;
                }

            }
        }
        public ResponseVM Update(ITGroupVM itgroupVM)
        {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        ITGroup itgroupToBeUpdated = context.ITGroups.Find(itgroupVM.ITGroupid);
                        if (itgroupToBeUpdated == null)
                            return new ResponseVM("updated", false, "ITGroup", ResponseVM.DOES_NOT_EXIST);

                        itgroupToBeUpdated.ITGroupCode = itgroupVM.ITGroupCode;
                        itgroupToBeUpdated.ITGroupName = itgroupVM.ITGroupName;
                        context.SaveChanges();

                        dbTransaction.Commit();
                        return new ResponseVM("updated", true, "ITGroup");
                    }
                    catch (Exception ex)
                    {
                        dbTransaction.Rollback();
                        return new ResponseVM("updated", false, "ITGroup", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
                    }
                }
            }
        }
        
    }
}