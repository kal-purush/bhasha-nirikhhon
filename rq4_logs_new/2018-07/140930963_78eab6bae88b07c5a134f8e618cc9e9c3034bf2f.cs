using System.Collections.Generic;
using SQLite;

using NotifyMe.Models.DbModels;
using NotifyMe.ServiceInterfaces;
using Xamarin.Forms;
using System.Linq;
using System.Threading.Tasks;

namespace NotifyMe.Services
{
    public class AlertService : IAlertService
    {
        private SQLiteConnection _dbContext;

        public AlertService()
        {
            _dbContext = DependencyService.Get<ISqliteConnection>().GetConnection();
            _dbContext.CreateTable<Alert>();
        }

        public int AddAlert(Alert alert)
        {  
            return _dbContext.Insert(alert);
        }

        public Alert GetAlertById(int id)
        {
            var alert = _dbContext.Table<Alert>().Where(a => a.Id == id).FirstOrDefault();
            return alert;
        }

        public List<Alert> GetAllLocationAlerts()
        {
            var alerts = _dbContext.Table<Alert>().Where(a => a.Type == true).ToList();
            return alerts;
        }

        public List<Alert> GetAllTimeAlerts()
        {
            var alerts = _dbContext.Table<Alert>().Where(a => a.Type == false).ToList();
            return alerts;
        }

        public List<Alert> GetAllUserLocationAlerts(string userEmail)
        {
            var alerts = _dbContext.Table<Alert>().Where(a => a.Type == true && a.User == userEmail).ToList();
            return alerts;
        }

        public List<Alert> GetAllUserTimeAlerts(string userEmail)
        {
            var alerts = _dbContext.Table<Alert>().Where(a => a.Type == false && a.User == userEmail).ToList();
            return alerts;
        }
    }
}