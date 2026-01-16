using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace TaskTracker.Entities
{
    [Serializable]
    class TaskItem
    {
        static List<TaskItem> tasks;
        public enum StatusEnum
        {
            todo=1,
            inProgress=2,
            done=3
        }

        #region Fields
        int id;
        string description;
        int status;
        DateTime createdAt;
        DateTime updatedAt;
        #endregion
        #region Props
        public int Id
        {
            get
            {
                return id;
            }
            set
            {
                id = value;
            }
        }
        public string Description
        {
            get
            {
                return description;
            }
            set
            {
                description = value;
            }
        }
        public int Status
        {
            get
            {
                return status;
            }
            set
            {
                status = value;
            }
        }
        public DateTime CreatedAt
        {
            get
            {
                return createdAt;
            }
            set
            {
                createdAt = value;
            }
        }
        public DateTime UpdatedAt
        {
            get
            {
                return updatedAt;
            }
            set
            {
                updatedAt = value;
            }
        }
        public static List<TaskItem> Tasks
        {
            get
            {
                if (tasks == null)
                {
                    try
                    {
                        string data = FileHelper.ReadFile();
                        if (string.IsNullOrEmpty(data))
                            return new List<TaskItem>();
                        tasks = JsonSerializer.Deserialize<List<TaskItem>>(data);
                        tasks.OrderBy(x => x.CreatedAt);
                    }
                    catch (Exception e)
                    {
                        throw e;
                    }

                }
                return tasks;
            }
        }
        #endregion
        public string GetStatusName(int staus)
        {
            switch (staus)
            {
                case (int)StatusEnum.todo:
                    return "ToDo";
                case (int)StatusEnum.inProgress:
                    return "In Progress";
                case (int)StatusEnum.done:
                    return "Done";
            }
            return "Not Valid";
        }
        public static bool AddTask(TaskItem task)
        {
            if (task == null)
                return false;
            try
            {
                string data = JsonSerializer.Serialize<TaskItem>(task);
                FileHelper.AddTaskToFile(data);
                return true;
            }
            catch (Exception e)
            {
                throw e;
            }
        }
    }
}