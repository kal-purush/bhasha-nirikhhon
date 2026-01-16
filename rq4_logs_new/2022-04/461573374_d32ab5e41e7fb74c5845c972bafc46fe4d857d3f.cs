using System;
using System.Collections.Generic;
using System.Text;

namespace Office
{
    class CompareByAssignTaskAndLastName : IComparer<Employee>
    {
        public const string sortType = "Task Assigners and their Last Name then others";
        public int Compare(Employee x, Employee y)
        {
            if (x is IAssignTask && y is IAssignTask)
                return String.Compare(x.LastName, y.LastName);
            else if (x is IAssignTask)
                return -1;
            else if (y is IAssignTask)
                return -1;
            else
                return 1;
        }
    }
}