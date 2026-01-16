using EventPlanner.ViewModels;
using System;
using System.Collections.Generic;
using System.Text;

namespace EventPlanner.Models
{
    public enum EventType { WEDDING, BIRTHDAY }
    public class Event : ObservableObject
    {
        private string _Title;
        private EventType _Type;
        private string _Description;
        private DateTime _DateFrom;
        private DateTime _DateTo;

        public String Title
        {
            get => _Title;
            set { _Title = value; RaisePropertyChngedEvent("Title"); }
        }
        public EventType Type
        {
            get => _Type;
            set { _Type = value; RaisePropertyChngedEvent("Type"); }
        }
        public String Description
        {
            get => _Description;
            set { _Description = value; RaisePropertyChngedEvent("Description"); }
        }
        public DateTime DateFrom
        {
            get => _DateFrom;
            set { _DateFrom = value; RaisePropertyChngedEvent("DateFrom"); }
        }
        public DateTime DateTo
        {
            get => _DateTo;
            set { _DateTo = value; RaisePropertyChngedEvent("DateTo"); }
        }
        public Event(string title, EventType type, string description, DateTime dateFrom, DateTime dateTo)
        {
            Title = title;
            Type = type;
            Description = description;
            DateFrom = dateFrom;
            DateTo = dateTo;
        }
    }
}