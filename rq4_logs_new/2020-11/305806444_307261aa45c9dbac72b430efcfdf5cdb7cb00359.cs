using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Repair_Service.DAL;
using Repair_Service.Models;

namespace Repair_Service.Controllers
{
    public class MainPageController
    {

        private Database database;

        public MainPageController()
        {
            database = new MainDatabase();
        }


        /// <summary>
        /// Metoda, pozwalająca na asynchroniczne pobieranie wszystkich zleceń z bazy danych
        /// </summary>
        public async Task<ObservableCollection<Order>> GetAllOrdersAsync()
        {
            ObservableCollection<Order> orders = new ObservableCollection<Order>();

            await Task.Run(() => 
                orders = database.GetAllOrders()
            );

            return orders;
        }



    }
}