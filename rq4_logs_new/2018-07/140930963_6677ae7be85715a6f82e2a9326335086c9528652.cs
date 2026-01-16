using System.Collections.Generic;
using SQLite;

using NotifyMe.Models.DbModels;
using NotifyMe.ServiceInterfaces;
using Xamarin.Forms;
using System.Linq;
using System.Threading.Tasks;

namespace NotifyMe.Services
{
    public class LocationService : ILocationService
    {
        private SQLiteConnection _dbContext;

        public LocationService()
        {
            _dbContext = DependencyService.Get<ISqliteConnection>().GetConnection();
            _dbContext.CreateTable<Location>();
        }

        public int AddLocation(Location location)
        {  
            return _dbContext.Insert(location);
        }

        public int DeleteLocation(Location location)
        {
            return _dbContext.Delete(location);
        }

        public List<Location> GetAllLocations()
        {
            var locations = _dbContext.Table<Location>()
                            .OrderByDescending(l => l.Id)
                            .ToList();
            return locations;
        }

        public List<Location> GetAllLocationsById(int id)
        {
            var location = _dbContext.Table<Location>()
                             .Where(l => l.Id == id && l.IsDeleted == false)
                             .OrderByDescending(a => a.Id)
                             .ToList();
            return location;
        }

        public List<Location> GetAllLocationsByUser(string userEmail)
        {
            var locations = _dbContext.Table<Location>()
                            .Where(l => l.User == userEmail && l.IsDeleted == false)
                            .OrderByDescending(a => a.Id)
                            .ToList();
            return locations;
        }

        public Location GetLocationById(int id)
        {
            var location = _dbContext.Table<Location>()
                            .Where(l => l.Id == id && l.IsDeleted == false)
                            .FirstOrDefault();
            return location;
        }
    }
}