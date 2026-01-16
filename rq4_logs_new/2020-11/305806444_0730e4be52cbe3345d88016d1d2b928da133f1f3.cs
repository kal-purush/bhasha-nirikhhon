using Repair_Service.DAL;
using Repair_Service.Models;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Repair_Service.Controllers
{
    public class ClientsPageController
    {

        private Database database;

        public ClientsPageController()
        {
            database = ProxyDatabase.GetDatabase();
        }

        public async Task<ObservableCollection<Client>> GetClientsAsync()
        {
            ObservableCollection<Client> clients = new ObservableCollection<Client>();

            await Task.Run(() => clients = database.GetAllClients());

            return clients;
        }

    }
}