using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace EasyMicroservice.Utilities.Threading.Helpers
{
    /// <summary>
    /// helper methods of task
    /// </summary>
    public static class TaskHelper
    {
        /// <summary>
        /// get a task that is completed
        /// </summary>
        /// <returns></returns>
        public static Task GetCompletedTask()
        {
#if (NET45)
            return Task.Delay(0);
#else
            return Task.CompletedTask;
#endif
        }
    }
}