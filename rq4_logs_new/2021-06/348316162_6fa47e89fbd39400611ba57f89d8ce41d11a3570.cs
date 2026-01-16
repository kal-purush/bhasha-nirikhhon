using EventPlanner.ViewModels;
using System;
using System.Collections.Generic;
using System.Text;

namespace EventPlanner.Models
{
    public enum TaskStatus { TO_DO, IN_PROGRESS, DONE }
    public enum TaskType { GENERIC, SEATING }
    public class Task : ObservableObject
    {
        private TaskStatus _Status;
        private int _EventId;
        private string _Description;
        private Collaborator _Collaborator;
        private TaskType _Type;

        public TaskStatus Status
        {
            get => _Status;
            set { _Status = value; RaisePropertyChngedEvent("Status"); }
        }
        public int EventId
        {
            get => _EventId;
            set { _EventId = value; RaisePropertyChngedEvent("EventId"); }
        }
        public String Description
        {
            get => _Description;
            set { _Description = value; RaisePropertyChngedEvent("Description"); }
        }
        public Collaborator Collaborator
        {
            get => _Collaborator;
            set { _Collaborator = value; RaisePropertyChngedEvent("Collaborator"); }
        }
        public TaskType Type
        {
            get => _Type;
            set { _Type = value; RaisePropertyChngedEvent("Type"); }
        }
        public Task(TaskStatus status, int eventId, string description, Collaborator collaborator, TaskType type)
        {
            Status = status;
            EventId = eventId;
            Description = description;
            Collaborator = collaborator;
            Type = type;
        }
    }
}