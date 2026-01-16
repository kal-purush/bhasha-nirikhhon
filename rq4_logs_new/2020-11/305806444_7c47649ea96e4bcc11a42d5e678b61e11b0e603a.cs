using Repair_Service.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Repair_Service.Controllers
{
    public class EditClientPageController : PageController
    {

        public EditClientPageController() : base() { }

        /// <summary>
        /// Pozwala na asynchroniczną aktualizację informacji klienta
        /// </summary>
        /// <param name="client">Obiekt klasy Client</param>
        /// <returns></returns>
        public async Task UpdateClient(Client client)
        {
            await Task.Run(() => database.UpdateClient(client));
        }

    }
}