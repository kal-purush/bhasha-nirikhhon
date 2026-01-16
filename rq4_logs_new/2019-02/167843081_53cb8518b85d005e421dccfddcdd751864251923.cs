using _FinalProject.Model.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace Data.Interfaces
{
   public interface ICalendarRepository
    {
        //Create
        Calendar Create(Calendar newCalendar);

        //Read
        Calendar GetById(int calendarId);
        ICollection<User> GetUserById(string userId);
        ICollection<Robin> GetRobinById(int robinId);

        //Update
        Calendar Update(Calendar updatedCalendar);

        //Delete
        bool DeleteById(int calendarId);
    }
}