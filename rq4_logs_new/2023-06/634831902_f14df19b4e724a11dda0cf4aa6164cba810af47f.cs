using DIMS_Core.DataAccessLayer.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AsyncTask = System.Threading.Tasks.Task;

namespace DIMS_Core.DataAccessLayer.Interfaces
{
    public interface IUserTaskRepository : IRepository<UserTask>
    {
        AsyncTask SetUserTaskAsSuccess(int userId, int taskId);
        AsyncTask SetUserTaskAsFail(int userId, int taskId);
    }
}