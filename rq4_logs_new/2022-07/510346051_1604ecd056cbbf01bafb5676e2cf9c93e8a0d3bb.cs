using System;

namespace ConsoleApp1.Entities
{
    public class Task
    {
        public Task(string name)
        {
            
            Name = name;
            Guid Id = new Guid();
            TaskInfo = null;
            IsRunning = true;
        }

        public Task(string name, Task parentTask)
        {
            Name = name;
            parentTask.TaskInfo.SubTask.Add(new Task(name));
        }
        

        public string Name { get; set; }
        public TaskInfo TaskInfo { get; set; }
        public bool IsRunning { get; set; }
        public Guid Id { get; set; }
    }
}