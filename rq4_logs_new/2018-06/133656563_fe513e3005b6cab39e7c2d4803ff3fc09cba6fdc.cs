using project2._4.DAL;
using project2._4.Entities.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace project2._4.BL.Repositories
{
    public class UserSettingsRepository
    {
        private DatabaseContext db;

        public UserSettingsRepository()
        {
            this.db = new DatabaseContext();
        }

        public UserSettings GetSettings(Guid Id)
        {
            return db.UserSettings.Find(Id);
        }

        public UserSettings GetuserSettings(Guid UserId)
        {
            return db.UserSettings.Where(x => x.UserId.Equals(UserId)).FirstOrDefault();
        }

        public void AddUserSettings(UserSettings usrSett)
        {
            db.UserSettings.Add(usrSett);
            db.SaveChanges();
        }

        public void UpdateUserSettings(UserSettings usrSett)
        {
            db.UserSettings.Attach(usrSett);
            db.Entry(usrSett).State = System.Data.Entity.EntityState.Modified;
            db.SaveChanges();
        }

        public void DeleteUserSettings(UserSettings usrSett)
        {
            db.UserSettings.Remove(usrSett);
            db.SaveChanges();
        }
    }
}