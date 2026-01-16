using System;
using System.Collections.Generic;
using System.Text;
using TicketingSystem.BLL.Contracts;
using TicketingSystem.BLL.Helpers;
using TicketingSystem.DAL.Models;
using TicketingSystem.ViewModel.ViewModel;
using TicketingSystem.ViewModel.ViewModels;

namespace TicketingSystem.BLL.Services
{
    public class TicketService : IGenericService<TicketVM>
    {
        private ToViewModel toViewModel = new ToViewModel();
        private ToModel toModel = new ToModel();
        private readonly TicketingSystemContext context;

        //inject dependencies
        public TicketService(TicketingSystemContext _context)
        {
            context = _context;
        }

        public ResponseVM Create(TicketVM ticketVM)
        {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        ticketVM.Ticketid = Guid.NewGuid();
                        //converts to book type then save to context
                        context.Tickets.Add(toModel.Ticket(ticketVM));
                        context.SaveChanges();

                        //commits changes to db
                        dbTransaction.Commit();
                        return new ResponseVM("created", true, "Ticket");
                    }
                    catch (Exception ex)
                    {

                        dbTransaction.Rollback();
                        return new ResponseVM("created", false, "Ticket", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
                    }
                }
            }
        }

        public ResponseVM Update(TicketVM ticketVM)
        {
            using (context)
            {
                using (var dbTransaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        //find ticket from the database
                        Ticket ticketToBeUpdated = context.Tickets.Find(ticketVM.Ticketid);
                        //ends function if ticket does not exists
                        if (ticketToBeUpdated == null)
                        {
                            return new ResponseVM("updated", false, "Ticket", ResponseVM.DOES_NOT_EXIST);
                        }

                        //update changes then save to context
                        ticketToBeUpdated.Ticketid = ticketVM.Ticketid;
                        ticketToBeUpdated.DateOfRequest = ticketVM.DateOfRequest;
                        ticketToBeUpdated.FormOfCommu = ticketVM.FormOfCommu;
                        ticketToBeUpdated.ContactInfo = ticketVM.ContactInfo;
                        ticketToBeUpdated.RequestTitle = ticketVM.RequestTitle;
                        ticketToBeUpdated.RequestDesc = ticketVM.RequestDesc;
                        ticketToBeUpdated.RequestedBy = ticketVM.RequestedBy;
                        ticketToBeUpdated.TrackingSatus = ticketVM.TrackingSatus;
                        ticketToBeUpdated.ResponseTime = ticketVM.ResponseTime;
                        ticketToBeUpdated.ResolveTime = ticketVM.ResolveTime;
                        ticketToBeUpdated.IsUrgent = ticketVM.IsUrgent;
                        ticketToBeUpdated.IsOpen = ticketVM.IsOpen;
                        context.SaveChanges();

                        //commit changes to db
                        dbTransaction.Commit();
                        return new ResponseVM("updated", true, "Ticket");
                    }
                    catch (Exception ex)
                    {
                        dbTransaction.Rollback();
                        return new ResponseVM("updated", false, "Ticket", ResponseVM.SOMETHING_WENT_WRONG, "", ex);
                    }
                }
            }
        }
    }
}