using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using FXB.Data;
namespace FXB.Common
{
    public class QtTaskUtil
    {
        public static QtEmployee GetJobByQtTask(QtTask qtTask, string jobNumber)
        {
            if (!qtTask.AllQtEmployee.ContainsKey(jobNumber))
            {
                throw new CrashException(string.Format("在QT任务[{0}]里查找顾问[{1}]失败", qtTask.QtKey, jobNumber));
            }

            return qtTask.AllQtEmployee[jobNumber];
        }
        public static string GetJobLevelNameByQtTask(QtTask qtTask, string jobNumber)
        {
            return GetJobByQtTask(qtTask, jobNumber).JobGradeName;
        }

        public static Int64 GetJobDepartmentIdByQtTask(QtTask qtTask, string jobNumber)
        {
            return GetJobByQtTask(qtTask, jobNumber).DepartmentId;
        }

        public static QtLevel GetJobQtLevelByQtTask(QtTask qtTask, string jobNumber)
        {
            return GetJobByQtTask(qtTask, jobNumber).QtLevel;
        }
    }
}