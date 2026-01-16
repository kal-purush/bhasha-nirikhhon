using Repair_Service.Models;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Repair_Service.Controllers
{
    public class StatusesPageController : PageController
    {

        public StatusesPageController() : base()
        {

        }


        public async Task<ObservableCollection<Status>> GetStatusesAsync()
        {
            ObservableCollection<Status> statuses = new ObservableCollection<Status>();
            await Task.Run(() => statuses = database.GetStatuses());
            return statuses;
        }

        public async Task<bool> AddNewStatusAsync(Status status)
        {
            return await Task.Run(() => database.AddNewStatus(status));
        }

        public async Task<bool> UpdateStatusAsync(Status status)
        {
            return await Task.Run(() => database.UpdateStatus(status));
        }

        public async Task<bool> DeleteStatusAsync(Status status)
        {
            return await Task.Run(() => database.DeleteStatus(status));
        }

    }
}