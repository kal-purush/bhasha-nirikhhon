using PlaceSharer.DAL.Entities;
using PlaceSharer.DAL.EF;
using PlaceSharer.DAL.Interfaces;
using System;

namespace PlaceSharer.DAL.Repositories
{
    public class LocationManager : ILocationManager
    {
        ApplicationContext Database;

        public LocationManager(ApplicationContext db)
        {
            Database = db;
        }

        public void Create(Location item)
        {
            Database.Locations.Add(item);
        }

        public void Update(Location item)
        {
            Database.Entry(item).State = System.Data.Entity.EntityState.Modified;
        }

        public void Delete(Location item)
        {
            Location location = Database.Locations.Find(item);
            if (location != null)
                Database.Locations.Remove(item);
        }

        public void Dispose()
        {
            Database.Dispose();
        }
    }
}