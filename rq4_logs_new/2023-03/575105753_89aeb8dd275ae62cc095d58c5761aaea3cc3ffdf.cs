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
    public partial class RecommendTransfersRetirementResignation : System.Web.UI.Page
    {
        static List<TransfersRetirementResignationMain> mainList;
        static List<TransfersRetirementResignationMain> filterList;

        protected void Page_Load(object sender, EventArgs e)
        {
            this.UnobtrusiveValidationMode = System.Web.UI.UnobtrusiveValidationMode.None;

            if (!IsPostBack)
            {
                BindDataSource();
                BindDdlStatus();
                BindDdlType();
            }
        }

        private void BindDataSource()
        {
            DepartmentUnitPositionsController departmentUnitPositionsController = ControllerFactory.CreateDepartmentUnitPositionsController();
            DepartmentUnitPositions departmentUnitPositions = new DepartmentUnitPositions();
            DepartmentUnitPositions departmentUnitPositionsCheckAdmint = new DepartmentUnitPositions();
            departmentUnitPositionsCheckAdmint = departmentUnitPositionsController.departmentUnitPositionWithPID(Convert.ToInt32(Session["UserId"]), true);

            TransfersRetirementResignationMainController main = ControllerFactory.CreateTransfersRetirementResignationMainController();
            List<TransfersRetirementResignationMain> mainListIn = main.GetAllTransfersRetirementResignation(false);
            mainList = new List<TransfersRetirementResignationMain>();

            //foreach (var item in mainListIn)
            //{
            //    if (item.ParentId != 0 && item.ParentId != 1)
            //    {
            //        departmentUnitPositions = departmentUnitPositionsController.GetDepartmentUnitPositions(item.ParentId, false, false, false, false, false);
            //        if (departmentUnitPositions.SystemUserId == Convert.ToInt32(Session["UserId"]))
            //        {
            //            mainList.Add(item);
            //        }
            //    }
            //    else
            //    {
            //        //--------------------- get all requests with no parent id to admin------------------------
            //        if (departmentUnitPositionsCheckAdmint._DepartmentUnit.DepartmentUnitTypeId == 1 && Convert.ToInt32(Session["UserTypeId"]) == 1)
            //        {
            //            mainList.Add(item);
            //        }
            //    }

            //    if (item.RecomendParentId != 0 && item.RecomendParentId != 1)
            //    {
            //        departmentUnitPositions = departmentUnitPositionsController.GetDepartmentUnitPositions(item.RecomendParentId, false, false, false, false, false);
            //        if (departmentUnitPositions.SystemUserId == Convert.ToInt32(Session["UserId"]))
            //        {
            //            mainList.Add(item);
            //        }
            //    }

            //}


            mainList = mainListIn.ToList();
            filterList = mainList.ToList();

            GridView1.DataSource = filterList;
            GridView1.DataBind();

        }

        private void BindDdlStatus()
        {
            TransfersRetirementResignationStatusController statusController = ControllerFactory.CreateTransfersRetirementResignationStatusController();
            List<TransfersRetirementResignationStatus> status = statusController.GetAllStatus(false);

            ddlStatus.DataSource = status;
            ddlStatus.DataValueField = "Id";
            ddlStatus.DataTextField = "StatusName";
            ddlStatus.DataBind();
            ddlStatus.Items.Insert(0, new ListItem("All", ""));
        }

        private void BindDdlType()
        {
            RequestTypeController requestTypeController = ControllerFactory.CreateRequestTypeController();
            List<RequestType> type = requestTypeController.GetAllRequestType(false);

            ddltype.DataSource = type;
            ddltype.DataValueField = "Id";
            ddltype.DataTextField = "RequestTypeName";
            ddltype.DataBind();
            ddltype.Items.Insert(0, new ListItem("All", ""));
        }

        protected void btnView_Click(object sender, EventArgs e)
        {
            int rowIndex = ((GridViewRow)((LinkButton)sender).NamingContainer).RowIndex;
            int pagesize = GridView1.PageSize;
            int pageindex = GridView1.PageIndex;
            rowIndex = (pagesize * pageindex) + rowIndex;
            Response.Redirect("RecommendTransfersRetirementResignationView.aspx?Id=" + filterList[rowIndex].MainId);

        }

        protected void GridView1_PageIndexChanging(object sender, GridViewPageEventArgs e)
        {
            GridView1.PageIndex = e.NewPageIndex;
            BindDataSource();
        }



        protected void ddlStatus_SelectedIndexChanged(object sender, EventArgs e)
        {

            if (ddlStatus.SelectedValue == "1")
            {
                if (ddltype.SelectedValue == "1")
                {
                    filterList = mainList.Where(a => a.StatusId == 1 && a.RequestTypeId == 1).ToList();
                }
                else if (ddltype.SelectedValue == "2")
                {
                    filterList = mainList.Where(a => a.StatusId == 1 && a.RequestTypeId == 2).ToList();
                }
                else if (ddltype.SelectedValue == "3")
                {
                    filterList = mainList.Where(a => a.StatusId == 1 && a.RequestTypeId == 3).ToList();
                }
                else
                {
                    filterList = mainList.Where(a => a.StatusId == 1).ToList();
                }
                GridView1.DataSource = filterList;
            }
            else if (ddlStatus.SelectedValue == "2")
            {
                if (ddltype.SelectedValue == "1")
                {
                    filterList = mainList.Where(a => a.StatusId == 2 && a.RequestTypeId == 1).ToList();
                }
                else if (ddltype.SelectedValue == "2")
                {
                    filterList = mainList.Where(a => a.StatusId == 2 && a.RequestTypeId == 2).ToList();
                }
                else if (ddltype.SelectedValue == "3")
                {
                    filterList = mainList.Where(a => a.StatusId == 2 && a.RequestTypeId == 3).ToList();
                }
                else
                {
                    filterList = mainList.Where(a => a.StatusId == 2).ToList();
                }
                GridView1.DataSource = filterList;
            }
            else if (ddlStatus.SelectedValue == "3")
            {
                if (ddltype.SelectedValue == "1")
                {
                    filterList = mainList.Where(a => a.StatusId == 3 && a.RequestTypeId == 1).ToList();
                }
                else if (ddltype.SelectedValue == "2")
                {
                    filterList = mainList.Where(a => a.StatusId == 3 && a.RequestTypeId == 2).ToList();
                }
                else if (ddltype.SelectedValue == "3")
                {
                    filterList = mainList.Where(a => a.StatusId == 3 && a.RequestTypeId == 3).ToList();
                }
                else
                {
                    filterList = mainList.Where(a => a.StatusId == 3).ToList();
                }
                GridView1.DataSource = filterList;
            }
            else if (ddlStatus.SelectedValue == "4")
            {
                if (ddltype.SelectedValue == "1")
                {
                    filterList = mainList.Where(a => a.StatusId == 4 && a.RequestTypeId == 1).ToList();
                }
                else if (ddltype.SelectedValue == "2")
                {
                    filterList = mainList.Where(a => a.StatusId == 4 && a.RequestTypeId == 2).ToList();
                }
                else if (ddltype.SelectedValue == "3")
                {
                    filterList = mainList.Where(a => a.StatusId == 4 && a.RequestTypeId == 3).ToList();
                }
                else
                {
                    filterList = mainList.Where(a => a.StatusId == 4).ToList();
                }
                GridView1.DataSource = filterList;
            }
            else if (ddlStatus.SelectedValue == "5")
            {
                if (ddltype.SelectedValue == "1")
                {
                    filterList = mainList.Where(a => a.StatusId == 5 && a.RequestTypeId == 1).ToList();
                }
                else if (ddltype.SelectedValue == "2")
                {
                    filterList = mainList.Where(a => a.StatusId == 5 && a.RequestTypeId == 2).ToList();
                }
                else if (ddltype.SelectedValue == "3")
                {
                    filterList = mainList.Where(a => a.StatusId == 5 && a.RequestTypeId == 3).ToList();
                }
                else
                {
                    filterList = mainList.Where(a => a.StatusId == 5).ToList();
                }
                GridView1.DataSource = filterList;
            }
            else
            {
                if (ddltype.SelectedValue == "1")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 1).ToList();
                }
                else if (ddltype.SelectedValue == "2")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 2).ToList();
                }
                else if (ddltype.SelectedValue == "3")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 3).ToList();
                }
                else
                {
                    filterList = mainList;
                }
                GridView1.DataSource = filterList;
            }
            GridView1.DataBind();
        }

        protected void ddltype_SelectedIndexChanged(object sender, EventArgs e)
        {
            if (ddltype.SelectedValue == "1")
            {
                if (ddlStatus.SelectedValue == "1")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 1 && a.StatusId == 1).ToList();
                }
                else if (ddlStatus.SelectedValue == "2")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 1 && a.StatusId == 2).ToList();
                }
                else if (ddlStatus.SelectedValue == "3")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 1 && a.StatusId == 3).ToList();
                }
                else if (ddlStatus.SelectedValue == "4")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 1 && a.StatusId == 4).ToList();
                }
                else if (ddlStatus.SelectedValue == "5")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 1 && a.StatusId == 5).ToList();
                }
                else
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 1).ToList();
                }
                GridView1.DataSource = filterList;
            }
            else if (ddltype.SelectedValue == "2")
            {
                if (ddlStatus.SelectedValue == "1")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 2 && a.StatusId == 1).ToList();
                }
                else if (ddlStatus.SelectedValue == "2")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 2 && a.StatusId == 2).ToList();
                }
                else if (ddlStatus.SelectedValue == "3")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 2 && a.StatusId == 3).ToList();
                }
                else if (ddlStatus.SelectedValue == "4")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 2 && a.StatusId == 4).ToList();
                }
                else if (ddlStatus.SelectedValue == "5")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 2 && a.StatusId == 5).ToList();
                }
                else
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 2).ToList();
                }
                GridView1.DataSource = filterList;
            }
            else if (ddltype.SelectedValue == "3")
            {
                if (ddlStatus.SelectedValue == "1")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 3 && a.StatusId == 1).ToList();
                }
                else if (ddlStatus.SelectedValue == "2")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 3 && a.StatusId == 2).ToList();
                }
                else if (ddlStatus.SelectedValue == "3")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 3 && a.StatusId == 3).ToList();
                }
                else if (ddlStatus.SelectedValue == "4")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 3 && a.StatusId == 4).ToList();
                }
                else if (ddlStatus.SelectedValue == "5")
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 3 && a.StatusId == 5).ToList();
                }
                else
                {
                    filterList = mainList.Where(a => a.RequestTypeId == 3).ToList();
                }
                GridView1.DataSource = filterList;
            }
            else
            {
                if (ddlStatus.SelectedValue == "1")
                {
                    filterList = mainList.Where(a => a.StatusId == 1).ToList();
                }
                else if (ddlStatus.SelectedValue == "2")
                {
                    filterList = mainList.Where(a => a.StatusId == 2).ToList();
                }
                else if (ddlStatus.SelectedValue == "3")
                {
                    filterList = mainList.Where(a => a.StatusId == 3).ToList();
                }
                else if (ddlStatus.SelectedValue == "4")
                {
                    filterList = mainList.Where(a => a.StatusId == 4).ToList();
                }
                else if (ddlStatus.SelectedValue == "5")
                {
                    filterList = mainList.Where(a => a.StatusId == 5).ToList();
                }
                else
                {
                    filterList = mainList;
                }
                GridView1.DataSource = filterList;
            }
            GridView1.DataBind();
        }
    }
}