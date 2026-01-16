using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Class_Library;

namespace TermProject
{
    public partial class StorageOptions : System.Web.UI.Page
    {
        double storage;
        int index;
        string[] loginInfo = new string[2];
        string email = "";
        protected void Page_Load(object sender, EventArgs e)
        {
                if (Session["Login"] != null)
                {
                    lblDisplayText.Text = Session["User"].ToString() + "'s Storage Options";
                    loginInfo[0] = Session["User"].ToString();
                    loginInfo[1] = Session["Password"].ToString();
                    email = Session["User"].ToString();
                    stateLoggedIn();
                }
                else
                {
                    lblDisplayText.Text = "Error: Must Login in";
                }
            
        }

        protected void btnBack_Click(object sender, EventArgs e)
        {
            Response.Redirect("Main.aspx");
        }

        public void stateLoggedIn()
        {
            rdoStorageOptions.Visible = true;
            storage = Convert.ToDouble(Functions.getUserTotalSpace(loginInfo, email));
            storage = storage / 1000000;
            if(storage == 100)
            {
                rdoStorageOptions.SelectedIndex = 0; ;
                rdoStorageOptions.SelectedItem.Text = "100 MB (Free) -- Current Plan";
                index = 0;
            }
            else if (storage == 1000)
            {
                rdoStorageOptions.SelectedIndex = 1; ;
                rdoStorageOptions.SelectedItem.Text = "1 GB ($0.99/Month) -- Current Plan";
                index = 1;
            }
            else if (storage == 2000)
            {
                rdoStorageOptions.SelectedIndex = 2; ;
                rdoStorageOptions.SelectedItem.Text = "2 GB ($1.99/Month) -- Current Plan";
                index = 2;
            }
            else if (storage == 5000)
            {
                rdoStorageOptions.SelectedIndex = 3; ;
                rdoStorageOptions.SelectedItem.Text = "5 GB ($2.99/Month) -- Current Plan";
                index = 3;
            }
            else if (storage == 10000)
            {
                rdoStorageOptions.SelectedIndex = 4; ;
                rdoStorageOptions.SelectedItem.Text = "10 GB ($4.99/Month) -- Current Plan";
                index = 4;
            }
            else if (storage == 50000)
            {
                rdoStorageOptions.SelectedIndex = 5; ;
                rdoStorageOptions.SelectedItem.Text = "50 GB ($9.99/Month) -- Current Plan";
                index = 5;
            }

        }


        protected void btnChangePlan_Click(object sender, EventArgs e)
        {
            if (rdoStorageOptions.SelectedIndex == index)
            {
                lblDisplayText2.Text = "Must choose plan different from current plan";
            }
        }

    }
}