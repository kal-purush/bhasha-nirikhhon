using _FinalProject.Data.Context;
using _FinalProject.Model.Models;
using Data.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Data.Implementations.EFCoreRepositories
{
    public class EFCoreCalendarRepository : ICalendarRepository
    {
        public Calendar Create(Calendar newCalendar)
        {
            using (var db = new FinalProjectDBContext())
            {
                db.Calendars.Add(newCalendar);
                db.SaveChanges();
                return newCalendar;
            }
        }

        public bool DeleteById(int calendarId)
        {
            using (var db = new FinalProjectDBContext())
            {
                var calendar = GetById(calendarId);
                db.Calendars.Remove(calendar);
                db.SaveChanges();
                return true;
            }
        }

        public Calendar GetById(int calendarId)
        {
            using (var db = new FinalProjectDBContext())
            {
                return db.Calendars.Single(c => c.Id == calendarId);
            }
        }

        public ICollection<Calendar> GetRobinById(int robinId)
        {
            using (var db = new FinalProjectDBContext())
            {
                var robinCalendar = db.Calendars.Where(c => c.RobinId == robinId).ToList() as ICollection<Calendar>;
                return robinCalendar;
            }
        }

        public ICollection<Calendar> GetUserById(string userId)
        {
            using (var db = new FinalProjectDBContext())
            {
                var userCalendar = db.Calendars.Where(c => c.UserId == userId).ToList() as ICollection<Calendar>;
                return userCalendar;
            }
        }

        public Calendar Update(Calendar updatedCalendar)
        {
            using (var db = new FinalProjectDBContext())
            {
                var update = GetById(updatedCalendar.Id);
                db.Entry(update).CurrentValues.SetValues(updatedCalendar);
                db.SaveChanges();
                return update;
            }
        }
    }
}