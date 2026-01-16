using ManPowerCore.Common;
using ManPowerCore.Domain;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ManPowerCore.Infrastructure
{
    public interface EmployeeDetailsFromProgramPlanDAO
    {
        List<EmployeeDetailsFromProgramPlan> GetAllEmployeeDetailsFromProgramPlans(DBConnection dbConnection);

        EmployeeDetailsFromProgramPlan GetAllEmployeeDetailsFromProgramPlansByProgramPlanId(DBConnection dbConnection, int ProgramPlanId);
    }
    public class EmployeeDetailsFromProgramPlanDAOSqlImpl : EmployeeDetailsFromProgramPlanDAO
    {
        public List<EmployeeDetailsFromProgramPlan> GetAllEmployeeDetailsFromProgramPlans(DBConnection dbConnection)
        {
            if (dbConnection.dr != null)
                dbConnection.dr.Close();

            dbConnection.cmd.CommandText = "SELECT b.Id AS ProgramPlan_Id,a.Program_Target_Id AS Program_Target_Id,e.Name AS Emp_Name, a.Department_Unit_Possitions_Id,d.Name ,f.Name AS Dep_UnitType_Name,d.Parent_Id \r\n  FROM Program_Assignee a INNER JOIN Program_Plan b ON  a.Program_Target_Id=b.Program_Target_Id" +
                "INNER JOIN Department_Unit_Possitions c ON c.Id= a.Department_Unit_Possitions_Id" +
                "INNER JOIN Department_Unit d ON d.Id=c.Department_Unit_Id" +
                "INNER JOIN Company_User e ON e.id=c.System_User_Id" +
                "INNER JOIN Department_Unit_Type f ON f.Id=d.Department_Unit_Type_Id ";

            dbConnection.dr = dbConnection.cmd.ExecuteReader();
            DataAccessObject dataAccessObject = new DataAccessObject();
            return dataAccessObject.ReadCollection<EmployeeDetailsFromProgramPlan>(dbConnection.dr);
        }

        public EmployeeDetailsFromProgramPlan GetAllEmployeeDetailsFromProgramPlansByProgramPlanId(DBConnection dbConnection, int ProgramPlanId)
        {
            if (dbConnection.dr != null)
                dbConnection.dr.Close();

            dbConnection.cmd.CommandText = "SELECT b.Id AS ProgramPlan_Id,a.Program_Target_Id AS Program_Target_Id,e.Name AS Emp_Name, a.Department_Unit_Possitions_Id,d.Name ,f.Name AS Dep_UnitType_Name,d.Parent_Id \r\n  FROM Program_Assignee a INNER JOIN Program_Plan b ON  a.Program_Target_Id=b.Program_Target_Id" +
                "INNER JOIN Department_Unit_Possitions c ON c.Id= a.Department_Unit_Possitions_Id" +
                "INNER JOIN Department_Unit d ON d.Id=c.Department_Unit_Id" +
                "INNER JOIN Company_User e ON e.id=c.System_User_Id" +
                "INNER JOIN Department_Unit_Type f ON f.Id=d.Department_Unit_Type_Id WHERE =" + ProgramPlanId + "";

            dbConnection.dr = dbConnection.cmd.ExecuteReader();
            DataAccessObject dataAccessObject = new DataAccessObject();
            return dataAccessObject.GetSingleOject<EmployeeDetailsFromProgramPlan>(dbConnection.dr);
        }
    }
}