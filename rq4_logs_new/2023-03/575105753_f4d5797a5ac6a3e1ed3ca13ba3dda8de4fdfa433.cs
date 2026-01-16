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
    public partial class MaintenanceRecomandView : System.Web.UI.Page
    {
        List<VehicleMeintenance> vehicleMeintenances = new List<VehicleMeintenance>();
        List<SystemUser> systemUsers = new List<SystemUser>();
        List<SystemUser> fiterList = new List<SystemUser>();

        protected void Page_Load(object sender, EventArgs e)
        {
            this.UnobtrusiveValidationMode = System.Web.UI.UnobtrusiveValidationMode.None;


            MaintenanceCategoryController maintenanceCategoryController = ControllerFactory.CreateMaintenanceCategoryController();

            VehicleMaintenanceController vehicleMaintenanceController = ControllerFactory.CreateVehicleMaintenanceController();
            vehicleMeintenances = vehicleMaintenanceController.GetAllVehicleMeintenance();

            SystemUserController systemUserController = ControllerFactory.CreateSystemUserController();
            systemUsers = systemUserController.GetAllSystemUser(false, false, false);

            string id = Request.QueryString["id"];

            butonA.Visible = false;
            butonR.Visible = false;

            foreach (var j in systemUsers.Where(u => u.SystemUserId == Convert.ToInt32(Session["UserId"])))
            {
                foreach (var i in vehicleMeintenances.Where(u => u.VehicleMeintenanceId == int.Parse(id)))
                {
                    fielNo.Text = i.FileNo;
                    date.Text = i.RequestDate.ToString();
                    requestedBy.Text = i.RequestDate.ToString();
                    vNo.Text = i.VehicleNumber;
                    description.Text = i.RequestDescription.ToString();

                    MaintenanceCategory maintenanceCategory = maintenanceCategoryController.GetMaintenanceCategory(i.CategoryId);
                    category.Text = maintenanceCategory.MaintenanceCategoryName;

                    if (i.IsApproved == 0)
                    {
                        butonA.Visible = true;
                        butonR.Visible = true;

                        approval.Text = "Not Recommended";
                    }
                    else if (i.IsApproved == 1)
                    {
                        approval.Text = "Pending Approval";
                    }

                    else if (i.IsApproved == 2)
                    {
                        approval.Text = "Request Approved";
                    }

                    else if (i.IsApproved == 3)
                    {
                        approval.Text = "Request Rejected";
                    }
                }
            }

        }

        protected void isClicked(object sender, EventArgs e)
        {
            Response.Redirect("MaintenanceRecomand.aspx");
        }

        protected void Accept(object sender, EventArgs e)
        {
            string id = Request.QueryString["id"];

            VehicleMaintenanceController vehicleMaintenanceController = ControllerFactory.CreateVehicleMaintenanceController();
            int result = vehicleMaintenanceController.UpdateRecommandationStatus(int.Parse(id), 1, Convert.ToInt32(Session["UserId"]), "");

            if (result == 1)
            {
                ClientScript.RegisterClientScriptBlock(this.GetType(), "alert", "swal('Success!', 'Sending to Recommondation..!', 'success');window.setTimeout(function(){window.location='MaintenanceRecomand.aspx'},2500);", true);
            }
            else
            {
                ClientScript.RegisterClientScriptBlock(this.GetType(), "alert", "swal('Error!', 'Something Went Wrong!', 'error');", true);

            }

        }

        protected void Reject(object sender, EventArgs e)
        {
            string id = Request.QueryString["id"];

            VehicleMaintenanceController vehicleMaintenanceController = ControllerFactory.CreateVehicleMaintenanceController();
            int result = vehicleMaintenanceController.UpdateRecommandationStatus(int.Parse(id), 3, Convert.ToInt32(Session["UserId"]), rejectReason.Text);

            if (result == 1)
            {
                ClientScript.RegisterClientScriptBlock(this.GetType(), "alert", "swal('Success!', 'Request Rejected..!', 'success');window.setTimeout(function(){window.location='MaintenanceRecomand.aspx'},2500);", true);
            }
            else
            {
                ClientScript.RegisterClientScriptBlock(this.GetType(), "alert", "swal('Error!', 'Something Went Wrong!', 'error');", true);

            }

        }

    }
}