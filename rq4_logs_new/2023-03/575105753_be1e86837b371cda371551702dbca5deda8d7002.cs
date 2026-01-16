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
    public partial class AddSalaryIncrement : System.Web.UI.Page
    {
        EmployeeController employeeController = ControllerFactory.CreateEmployeeController();
        public List<Employee> EmployeeList = new List<Employee>();

        SalaryIncrementController salaryIncrementController = ControllerFactory.CreateSalaryIncrementController();
        SalaryIncrement salaryIncrementObj = new SalaryIncrement();
        protected void Page_Load(object sender, EventArgs e)
        {
            this.UnobtrusiveValidationMode = System.Web.UI.UnobtrusiveValidationMode.None;

            if (!IsPostBack)
            {
                EmployeeList = employeeController.GetAllEmployees();
                ddlEmployee.DataSource = EmployeeList;
                ddlEmployee.DataBind();
            }
        }

        protected void btnSave_Click(object sender, EventArgs e)
        {
            salaryIncrementObj.EmployeeId = Convert.ToInt32(ddlEmployee.SelectedValue);
            salaryIncrementObj.SalaryIncrementStatusId = 1;
            salaryIncrementObj.CreatedDate = DateTime.Now;
            salaryIncrementObj.Allowances = Convert.ToInt32(txtAllowances.Text);
            salaryIncrementObj.BasicSalary = Convert.ToInt32(txtSalary.Text);
            salaryIncrementObj.TotalSalary = Convert.ToInt32(txtToal.Text);
            salaryIncrementObj.CreatedUser = Session["UserId"].ToString();

            salaryIncrementController.Save(salaryIncrementObj);

            string url = "SalaryIncrements.aspx";
            Response.Redirect(url);
        }
    }
}