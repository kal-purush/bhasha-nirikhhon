using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Class_Library;

namespace PBFrontEnd
{
    public partial class AddAppointment : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            if (IsPostBack == false)
            {
                DisplayMOTTimes();
            }
        }

        // function for populating the mot times drop down list
        void DisplayMOTTimes()
        {
            // create an instance of the MOT Time collection
            Class_Library.clsMOTTimeCollection MOTTimes = new Class_Library.clsMOTTimeCollection();
            // set the data source to the list of mot times in the collection
            ddlMOTTime.DataSource = MOTTimes.AllMOTTimes;
            // set the name of the primary key
            ddlMOTTime.DataValueField = "MOTTimeID";
            // set the data field to display
            ddlMOTTime.DataTextField = "MOTTime";
            // bind the data to the list
            ddlMOTTime.DataBind();
        }



        void Add()
        {
            // create an instance of the appointment book
            Class_Library.clsAppointmentCollection AppointmentBook = new Class_Library.clsAppointmentCollection();
            // validate the data on the web form
            String Error = AppointmentBook.ThisAppointment.Valid(txtMOTDate.Text, txtCarRegNo.Text, txtCustomerID.Text, txtStaffID.Text, txtJobID.Text);
            // if the data is OK then add it to the object
            if (Error == "")
            {
                // get the data entered by the user
                AppointmentBook.ThisAppointment.MOTDate = Convert.ToDateTime(txtMOTDate.Text);
                AppointmentBook.ThisAppointment.CarRegNo = txtCarRegNo.Text;
                AppointmentBook.ThisAppointment.CustomerID = Convert.ToInt32(txtCustomerID.Text);
                AppointmentBook.ThisAppointment.StaffID = Convert.ToInt32(txtStaffID.Text);
                AppointmentBook.ThisAppointment.JobID = Convert.ToInt32(txtJobID.Text);
                AppointmentBook.ThisAppointment.Active = chkActive.Checked;
                AppointmentBook.ThisAppointment.MOTTimeID = Convert.ToInt32(ddlMOTTime.SelectedValue);
                // add the record
                AppointmentBook.Add();
            }
            else
            {
                // report an error
                lblError.Text = "There were problems wit the data entered " + Error;
            }
        }

        // event handler for OK button
        protected void btnOK_Click(object sender, EventArgs e)
        {
            // add the new record
            Add();
            // all done so redirect back to the main page
            Response.Redirect("DefaultAppointment.aspx");
        }
    }
}