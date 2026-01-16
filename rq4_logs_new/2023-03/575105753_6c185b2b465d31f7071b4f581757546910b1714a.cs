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
    public partial class PlanningRecommendation1 : System.Web.UI.Page
    {

        static List<ProgramPlan> plansList = new List<ProgramPlan>();
        List<ProgramPlanApprovalDetails> ProgramPlanApprovalDetails = new List<ProgramPlanApprovalDetails>();
        List<ProjectPlanResource> projectPlanResourcesList = new List<ProjectPlanResource>();

        protected void Page_Load(object sender, EventArgs e)
        {
            if (!IsPostBack)
            {
                DataSourceBind();


            }
        }

        private void DataSourceBind()
        {
            ProgramPlanController programPlanController = ControllerFactory.CreateProgramPlanController();
            ProgramPlanApprovalDetailsController programPlanApprovalDetailsController = ControllerFactory.CreateProgramPlanApprovalDetailsController();

            ProgramPlanApprovalDetails = programPlanApprovalDetailsController.GetAll();

            plansList = programPlanController.GetAllProgramPlan();
            plansList = plansList.Where(x => x.ProjectStatusId == 2013).ToList();

            foreach (var item in plansList)
            {

                item._ProgramPlanApprovalDetails = ProgramPlanApprovalDetails.Where(u => u.ProgramPlanId == item.ProgramPlanId && u.Recommendation1By == Convert.ToInt32(Session["DepUnitPositionId"])).ToList();
            }

            gvProgramPlan.DataSource = plansList;
            gvProgramPlan.DataBind();


        }

        protected void btnView_Click(object sender, EventArgs e)
        {
            int rowIndex = ((GridViewRow)((LinkButton)sender).NamingContainer).RowIndex;
            int pagesize = gvProgramPlan.PageSize;
            int pageindex = gvProgramPlan.PageIndex;
            rowIndex = (pagesize * pageindex) + rowIndex;

            ProgramPlan programPlansListBind = new ProgramPlan();
            programPlansListBind = plansList[rowIndex];

            ProjectPlanResourceController projectPlanResourceController = ControllerFactory.CreateProjectPlanResourceController();
            projectPlanResourcesList = projectPlanResourceController.GetAllProjectPlanResourcesByProgramPlanId(programPlansListBind.ProgramPlanId);

            foreach (var item in projectPlanResourcesList)
            {
                for (int i = 0; i < chkList.Items.Count; i++)
                {
                    if (chkList.Items[i].Value == item.ResourcePersonId.ToString())
                    {
                        chkList.Items[i].Selected = true;
                    }
                }
            }

            txtProgramName.Text = programPlansListBind.ProgramName;
            txtDate.Text = programPlansListBind.Date.ToString("yyyy-MM-dd");
            txtBudget.Text = programPlansListBind.ApprovedAmount.ToString();
            txtFemaleCount.Text = programPlansListBind.FemaleCount.ToString();
            txtMaleCount.Text = programPlansListBind.MaleCount.ToString();
            txtTotalCount.Text = (programPlansListBind.FemaleCount + programPlansListBind.MaleCount).ToString();
            txtLocation.Text = programPlansListBind.Location.ToString();
            txtActualOutcome.Text = programPlansListBind.Outcome.ToString();
            txtActualOutput.Text = programPlansListBind.ActualOutput.ToString();
            txtExpenditure.Text = programPlansListBind.ActualAmount.ToString();



        }
    }
}