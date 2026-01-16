using ManPowerCore.Common;
using ManPowerCore.Controller;
using ManPowerCore.Domain;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace ManPowerWeb
{
    public partial class DME22Rec1 : System.Web.UI.Page
    {
        public int positionID;
        List<TaskAllocation> taskAllocationList = new List<TaskAllocation>();
        List<SystemUser> systemUserList = new List<SystemUser>();
        protected void Page_Load(object sender, EventArgs e)
        {
            positionID = Convert.ToInt32(Session["DepUnitPositionId"]);
            BindDataSource();
        }
        public void BindDataSource()
        {
            TaskAllocationController allocation = ControllerFactory.CreateTaskAllocationController();

            SystemUserController SystemUser = ControllerFactory.CreateSystemUserController();

            List<TaskAllocation> taskAllocationList1 = allocation.GetAllTaskAllocation(false, true, false, false);
            systemUserList = SystemUser.GetAllSystemUser(false, false, false);

            foreach (var item in taskAllocationList1)
            {
                if (item.DME22Rec1By == positionID && item.StatusId == 2011)
                {
                    taskAllocationList.Add(item);
                }
            }

            foreach (var item in taskAllocationList)
            {
                item._SystemUser = systemUserList.Where(x => x.SystemUserId == item._DepartmentUnitPositions.SystemUserId).Single();
            }

            gvDME22Rec1.DataSource = taskAllocationList;
            gvDME22Rec1.DataBind();
        }

        protected void btnAction_Click(object sender, EventArgs e)
        {
            GridViewRow gv = (GridViewRow)((LinkButton)sender).NamingContainer;

            int rowIndex = ((GridViewRow)((LinkButton)sender).NamingContainer).RowIndex;

            string url = "DME22Rec1Render.aspx?" + "taskAllocationID=" + taskAllocationList[rowIndex].TaskAllocationId;
            Response.Redirect(url);
        }
    }
}