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
    public partial class SystemFunctions : System.Web.UI.Page
    {
        static List<AutFunction> autFunctionsList = new List<AutFunction>();

        protected void Page_Load(object sender, EventArgs e)
        {
            this.UnobtrusiveValidationMode = System.Web.UI.UnobtrusiveValidationMode.None;

            if (!IsPostBack)
            {
                BindFuntions();
            }
        }

        private void BindFuntions()
        {
            AutFunctionController autFunctionController = ControllerFactory.CreateAutFunctionController();
            autFunctionsList = autFunctionController.GetAllAutFunction();

            gvSystemFunctions.DataSource = autFunctionsList;
            gvSystemFunctions.DataBind();

        }
        protected void btnChange_Click(object sender, EventArgs e)
        {
            int rowIndex = ((GridViewRow)((LinkButton)sender).NamingContainer).RowIndex;

        }

        protected void gvSystemFunctions_RowEditing(object sender, GridViewEditEventArgs e)
        {
            gvSystemFunctions.EditIndex = e.NewEditIndex;

            int rowIndex = e.NewEditIndex;
            int division = autFunctionsList[rowIndex].division;

            gvSystemFunctions.DataSource = ControllerFactory.CreateAutFunctionController().GetAllAutFunction();
            gvSystemFunctions.DataBind();
        }

        protected void gvSystemFunctions_RowCancelingEdit(object sender, GridViewCancelEditEventArgs e)
        {

            gvSystemFunctions.EditIndex = -1;
            gvSystemFunctions.DataSource = ControllerFactory.CreateAutFunctionController().GetAllAutFunction();
            gvSystemFunctions.DataBind();
        }

        protected void gvSystemFunctions_RowUpdating(object sender, GridViewUpdateEventArgs e)
        {

            Label Id = gvSystemFunctions.Rows[e.RowIndex].FindControl("lblID") as Label;
            DropDownList ddlDivision = gvSystemFunctions.Rows[e.RowIndex].FindControl("ddlDivision") as DropDownList;
            TextBox txtOrderNum = gvSystemFunctions.Rows[e.RowIndex].FindControl("txtOrderNum") as TextBox;
            TextBox txtMenu = gvSystemFunctions.Rows[e.RowIndex].FindControl("txtMenu") as TextBox;

            if (ddlDivision.SelectedValue != "")
            {

                AutFunctionController autFunctionController = ControllerFactory.CreateAutFunctionController();
                AutFunction autFunction = autFunctionsList.Where(x => x.AutFunctionId == Convert.ToUInt32(Id.Text)).Single();
                autFunction.division = Convert.ToInt32(ddlDivision.SelectedValue);
                autFunction.OrderNumber = Convert.ToInt32(txtOrderNum.Text);
                autFunction.MenuIcon = txtMenu.Text;

                int output = 0;
                output = autFunctionController.Update(autFunction);
                if (output == 0)
                {
                    ClientScript.RegisterClientScriptBlock(this.GetType(), "alert", "swal('Failed!', 'Something Went Wrong!', 'error')", true);
                }
                else
                {
                    gvSystemFunctions.EditIndex = -1;
                    gvSystemFunctions.DataSource = ControllerFactory.CreateAutFunctionController().GetAllAutFunction();
                    gvSystemFunctions.DataBind();
                    ClientScript.RegisterClientScriptBlock(this.GetType(), "alert", "swal('Success!', 'Updated Succesfully!', 'success')", true);
                }
            }

        }
    }
}