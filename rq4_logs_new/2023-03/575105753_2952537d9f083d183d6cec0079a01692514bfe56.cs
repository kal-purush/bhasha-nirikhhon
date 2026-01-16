using ManPowerCore.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ManPowerCore.Domain
{
    [Serializable]
    public class EmployeeDetailsFromProgramPlan
    {
        [DBField("ProgramPlan_Id")]
        public int ProgramPlanId { get; set; }
        [DBField("Program_Target_Id")]
        public int ProgramTargetId { get; set; }

        [DBField("Emp_Name")]
        public string EmployeeName { get; set; }

        [DBField("Department_Unit_Possitions_Id")]
        public int DepartmentUnitPositionId { get; set; }


        [DBField("Name")]
        public string DivisionName { get; set; }

        [DBField("Dep_UnitType_Name")]
        public string DepTypeName { get; set; }

        [DBField("Parent_Id")]
        public int ParentId { get; set; }
    }
}