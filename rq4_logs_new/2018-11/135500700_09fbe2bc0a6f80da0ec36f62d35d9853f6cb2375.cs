using System;
using System.Linq;
using ToDoApp.Models;

namespace ToDoApp.Services
{
    public interface IRepository : IDisposable
    {
        IQueryable<Status> Statuses { get; }
        IQueryable<ToDo> ToDos { get; }

        void Add(Status status);
        void Add(ToDo toDo);
        void DeleteStatus(int id);
        void DeleteToDo(int id);
        Status GetStatus(int id);
        ToDo GetToDo(int id);
        void Update(int id, Status status);
        void Update(int id, ToDo toDo);
    }
}