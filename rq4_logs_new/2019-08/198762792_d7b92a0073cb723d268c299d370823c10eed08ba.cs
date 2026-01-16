using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using TicketingSystem.BLL.Helpers;
using TicketingSystem.DAL.Models;
using TicketingSystem.ViewModel.ViewModel;
using TicketingSystem.ViewModel.ViewModels;
using TicketingSystem.BLL.Contracts;

namespace TicketingSystem.BLL.Services
{
    public class SeverityService : IGenericService<SeverityVM>
    { 
        private ToViewModel toViewModel = new ToViewModel();
        private ToModel toModel = new ToModel();
        private readonly TicketingSystemContext context;
        public SeverityService(TicketingSystemContext _context)
        {
            context = _context;
        }

        public ResponseVM Create(SeverityVM severityVM)
        {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        severityVM.ID = Guid.NewGuid();
                        context.Severities.Add(toModel.Severity(severityVM));
                        context.SaveChanges();

                        //commit change to db
                        dbTransaction.Commit();
                        return new ResponseVM("create", true, "Severity");
                    }
                    catch (Exception ex)
                    {
                        //rollback changes
                        dbTransaction.Rollback();
                        return new ResponseVM("create", false, "Severity", ResponseVM.SOMETHING_WENT_WRONG, "", ex);

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
                        Severity severityToBeDeleted = context.Severities.Find(id);
                        if (severityToBeDeleted == null)
                        {
                            return new ResponseVM("deleted", false, "Severity", ResponseVM.DOES_NOT_EXIST);
                        }

                        // delete from database
                        context.Severities.Remove(severityToBeDeleted);
                        context.SaveChanges();

                        dbTransaction.Commit();
                        return new ResponseVM("deleted", true, "Severity");
                    }
                    catch (Exception ex)
                    {
                        //rollback changes
                        dbTransaction.Rollback();
                        return new ResponseVM("deleted", false, "Severity", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
                    }
                }
            }
        }


        public IEnumerable<SeverityVM> GetAll()
        {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        var severities = context.Severities.ToList().OrderByDescending(x => x.ID);
                        var severityVm = severities.Select(x => toViewModel.Severity(x));
                        return severityVm;
                    }
                    catch (Exception)
                    {

                        throw;
                    }
                }
            }
        }


        public SeverityVM GetSingleBy(Guid id)
        {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        // select * from severities where id = 'id'
                        var severity = context.Severities.Find(id);
                        // pag gusto specific column by title ng severity ito ung itype
                        //var vooksTitle = context.Severities.Where(x => x.Title == "");
                        SeverityVM severityVM = null;
                        if (severity != null) severityVM = toViewModel.Severity(severity);
                        return severityVM;
                    }
                    catch (Exception)
                    {

                        throw;
                    }
                }
            }
        }

        public ResponseVM Update(SeverityVM severityVM)
        {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        //find severity from the databse
                        Severity severityToBeUpdate = context.Severities.Find(severityVM.ID);
                        if (severityToBeUpdate == null)
                            return new ResponseVM("updated", false, "Severity", ResponseVM.DOES_NOT_EXIST);

                        // update changes
                        severityToBeUpdate.SeverityCode = severityVM.SeverityCode;
                        severityToBeUpdate.SeverityDesc = severityVM.SeverityDesc;
                        context.SaveChanges();
                        dbTransaction.Commit();
                        return new ResponseVM("updated", true, "Severity");
                    }
                    catch (Exception ex)
                    {

                        dbTransaction.Rollback();
                        return new ResponseVM("updated", false, "Severity", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
                    }
                }
            }
        }
    }
}