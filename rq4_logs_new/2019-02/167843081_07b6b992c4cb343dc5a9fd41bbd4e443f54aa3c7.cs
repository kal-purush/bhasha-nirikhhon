using System;
using _FinalProject.Model.Models;
using System.Collections.Generic;
using System.Text;

namespace Service.Services
{
    public interface ICalendarService
    {
        //Create
        Calendar Create(Calendar newCalendar);

        //Read
        Calendar GetById(int calendarId);
        ICollection<Calendar> GetUserById(string userId);
        ICollection<Calendar> GetRobinById(int robinId);

        //Update
        Calendar Update(Calendar updatedCalendar);

        //Delete
        bool DeleteById(int calendarId);
    }

    public class CalendarService : ICalendarService
    {
        private readonly ICalendarService _calendarService;

        public CalendarService(ICalendarService calendarService) =>
            _calendarService = calendarService;

        public Calendar Create(Calendar newCalendar) =>
            _calendarService.Create(newCalendar);

        public bool DeleteById(int calendarId) =>
            _calendarService.DeleteById(calendarId);

        public Calendar GetById(int calendarId) =>
            _calendarService.GetById(calendarId);

        public ICollection<Calendar> GetRobinById(int robinId) =>
            _calendarService.GetRobinById(robinId);

        public ICollection<Calendar> GetUserById(string userId) =>
            _calendarService.GetUserById(userId);

        public Calendar Update(Calendar updatedCalendar) =>
            _calendarService.Update(updatedCalendar);      
    }
}