using System;
using System.Collections.Generic;
using System.Linq;
using ConsoleApp1.Entities;

namespace ConsoleApp1.Servise
{
    public class TaskManager
    {
        private List<Task> _allTasks = new List<Task>();
        private List<TaskGroup> _allGroups = new List<TaskGroup>();

        public Task CreateTask(string name, string description = null, DateTime deadline = default)
        {
            var buffer = new Task(name,description,deadline);
            _allTasks.Add(buffer);
            return buffer;
        }
        
        public TaskGroup CreateGroup(string name)
        {
            var buffer = new TaskGroup(name);
            _allGroups.Add(buffer);
            return buffer;
        }

        public List<Task> ListAllTasks()
        {
            return _allTasks;
        }

        public int DeleteTask(Task task)
        {
            if (task.TaskInfo.TaskGroup != null)
            {
                foreach (var taskGroup in _allGroups.Where(taskGroup => taskGroup.Id == task.TaskInfo.TaskGroup.Id))
                {
                    taskGroup.Tasks.Remove(task);
                }
            }

            if (task.TaskInfo.SubTask != null)
            {
                foreach (var subtask in task.TaskInfo.SubTask)
                {
                    _allTasks.Remove(subtask);
                }
            }
            _allTasks.Remove(task);
            return 0;
        }

        public List<Task> GetCompletedTasks()
        {
            return _allTasks.Where(task => task.Complete == true).ToList();
        }

        public List<Task> GetTodayTasks()
        {
            return _allTasks.Where(task => task.TaskInfo.Deadline == DateTime.Today).ToList();
        }

        public int DeleteTaskGroup(TaskGroup taskGroup)
        {
            if (taskGroup.Tasks != null)
            {
                foreach (var task in taskGroup.Tasks)
                {
                    task.TaskInfo.TaskGroup = null;
                }
            }

            _allGroups.Remove(taskGroup);
            return 0;
        }
        
        public List<TaskGroup> ListAllTasksGroups()
        {
            return _allGroups;
        }
    }
}