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
    public partial class RecommendationLeave : System.Web.UI.Page
    {
        List<SystemUser> systemUsers = new List<SystemUser>();
        List<DepartmentUnit> departmentUnitsList = new List<DepartmentUnit>();
        List<Employee> employeesList = new List<Employee>();
        List<StaffLeave> staffLeaveList = new List<StaffLeave>();
        List<StaffLeave> staffLeaveSearchList = new List<StaffLeave>();
        protected void Page_Load(object sender, EventArgs e)
        {
            this.UnobtrusiveValidationMode = System.Web.UI.UnobtrusiveValidationMode.None;

            if (!IsPostBack)
            {
                bindDataSource();
            }
        }

        private void bindDataSource()
        {
            DepartmentUnitController departmentUnitController = ControllerFactory.CreateDepartmentUnitController();
            departmentUnitsList = departmentUnitController.GetAllDepartmentUnit(false, false);

            ddlHo.DataSource = departmentUnitsList.Where(x => x.DepartmentUnitTypeId == 1);
            ddlHo.DataValueField = "DepartmentUnitId";
            ddlHo.DataTextField = "Name";
            ddlHo.DataBind();

            ddlDistrict.DataSource = departmentUnitsList.Where(x => x.DepartmentUnitTypeId == 2);
            ddlDistrict.DataValueField = "DepartmentUnitId";
            ddlDistrict.DataTextField = "Name";
            ddlDistrict.DataBind();
            ddlDistrict.Items.Insert(0, new ListItem("Select District", ""));



            StaffLeaveController staffLeaveController = ControllerFactory.CreateStaffLeaveControllerImpl();


            staffLeaveList = staffLeaveController.getStaffLeaves(true);

            ViewState["staffLeaveList"] = staffLeaveList.ToList();



            gvApproveLeave.DataSource = staffLeaveList;
            gvApproveLeave.DataBind();


        }

        protected void ddlDistrict_SelectedIndexChanged(object sender, EventArgs e)
        {
            bindDSDivision();
        }

        private void bindDSDivision()
        {
            DepartmentUnitController departmentUnitController = ControllerFactory.CreateDepartmentUnitController();
            departmentUnitsList = departmentUnitController.GetAllDepartmentUnit(false, false);

            ddlDS.DataSource = departmentUnitsList.Where(x => x.DepartmentUnitTypeId == 3 && x.ParentId == int.Parse(ddlDistrict.SelectedValue));
            ddlDS.DataValueField = "DepartmentUnitId";
            ddlDS.DataTextField = "Name";
            ddlDS.DataBind();
            ddlDS.Items.Insert(0, new ListItem("Select DS Division", ""));

        }

        protected void gvApproveLeave_RowDataBound(object sender, GridViewRowEventArgs e)
        {
            if (e.Row.RowType == DataControlRowType.DataRow)
            {
                e.Row.Cells[0].Text = DateTime.Parse(e.Row.Cells[0].Text).ToShortDateString();
                e.Row.Cells[1].Text = DateTime.Parse(e.Row.Cells[1].Text).ToShortDateString();
            }
        }


        protected void btnView_Click(object sender, EventArgs e)
        {
            int rowIndex = ((GridViewRow)((LinkButton)sender).NamingContainer).RowIndex;
            int pagesize = gvApproveLeave.PageSize;
            int pageindex = gvApproveLeave.PageIndex;
            rowIndex = (pagesize * pageindex) + rowIndex;

            StaffLeaveController staffLeaveController = ControllerFactory.CreateStaffLeaveControllerImpl();
            staffLeaveList = staffLeaveController.getStaffLeaves(true);

            Response.Redirect("RecommendationLeaveView.aspx?EmpId=" + staffLeaveList[rowIndex].EmployeeId.ToString() + "&Id=" + staffLeaveList[rowIndex].StaffLeaveId);


        }

        protected void btnSearch_Click(object sender, EventArgs e)
        {



            staffLeaveSearchList = (List<StaffLeave>)ViewState["staffLeaveList"];

            if (ddlDistrict.SelectedValue != "")
            {
                if (ddlDS.SelectedValue != "")
                {
                    staffLeaveSearchList = staffLeaveSearchList.Where(x => x._EMployeeDetails.DistrictId == Convert.ToInt32(ddlDistrict.SelectedValue) && x._EMployeeDetails.DSDivisionId == Convert.ToInt32(ddlDS.SelectedValue)).ToList();

                }
                else
                {
                    staffLeaveSearchList = staffLeaveSearchList.Where(x => x._EMployeeDetails.DistrictId == Convert.ToInt32(ddlDistrict.SelectedValue)).ToList();

                }
            }
            else
            {
                staffLeaveSearchList = staffLeaveSearchList.Where(x => x._EMployeeDetails.UnitType == Convert.ToInt32(ddlHo.SelectedValue)).ToList();
            }




            gvApproveLeave.DataSource = staffLeaveSearchList;
            gvApproveLeave.DataBind();

        }

        protected void gvApproveLeave_PageIndexChanging(object sender, GridViewPageEventArgs e)
        {
            gvApproveLeave.PageIndex = e.NewPageIndex;
            this.bindDataSource();
        }
    }
}