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
    public partial class RecommendationTrainingRequest : System.Web.UI.Page
    {
        List<TrainingRequests> trainingRequestsList = new List<TrainingRequests>();
        TrainingRequests trainingRequestObj = new TrainingRequests();
        public int depPositionID;

        protected void Page_Load(object sender, EventArgs e)
        {
            this.UnobtrusiveValidationMode = System.Web.UI.UnobtrusiveValidationMode.None;

            depPositionID = Convert.ToInt32(Session["DepUnitPositionId"]);
            BindDataSource();
        }

        public void BindDataSource()
        {
            TrainingRequestsController trainingRequestsController = ControllerFactory.CreateTrainingRequestsController();
            trainingRequestsList = trainingRequestsController.GetAllTrainingRequestsWithDetail();

            trainingRequestsList = trainingRequestsList.Where(x => x.Accepted_User == depPositionID && x.Is_Active == 1 && x.ProjectStatusId == 1 && x.Trainingmain.Start_Date > DateTime.Now).ToList();

            gvApproveTraining.DataSource = trainingRequestsList;
            gvApproveTraining.DataBind();
        }

        protected void btnApprove_Click(object sender, EventArgs e)
        {
            GridViewRow gv = (GridViewRow)((LinkButton)sender).NamingContainer;

            int rowIndex = ((GridViewRow)((LinkButton)sender).NamingContainer).RowIndex;

            TrainingRequestsController trainingRequestsController = ControllerFactory.CreateTrainingRequestsController();

            trainingRequestObj = trainingRequestsList[rowIndex];

            trainingRequestObj.Recommend_date = DateTime.Now;
            trainingRequestObj.ProjectStatusId = 2;

            trainingRequestsController.Update(trainingRequestObj);

            string url = "approvetrainingrequest.aspx";
            Response.Redirect(url);
        }

        protected void btnReject_Click(object sender, EventArgs e)
        {
            GridViewRow gv = (GridViewRow)((LinkButton)sender).NamingContainer;

            int rowIndex = ((GridViewRow)((LinkButton)sender).NamingContainer).RowIndex;

            TrainingRequestsController trainingRequestsController = ControllerFactory.CreateTrainingRequestsController();

            trainingRequestObj = trainingRequestsList[rowIndex];

            trainingRequestObj.Recommend_date = DateTime.Now;
            trainingRequestObj.ProjectStatusId = 7;

            trainingRequestsController.Update(trainingRequestObj);

            string url = "approvetrainingrequest.aspx";
            Response.Redirect(url);
        }
    }
}