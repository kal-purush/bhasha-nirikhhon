using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace EmployeeWebClient
{
    public partial class Default : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {

        }

        protected void GetEmployee_Button_Click(object sender, EventArgs e)
        {
            try
            {
                EmployeeServiceClient client = new EmployeeServiceClient();

                var employee = client.GetEmployee(int.Parse(EnterEmployeeID_TextBox.Text));
                DisplayEmployee_TextBox.Text = employee.EmployeeId.ToString();
                LastName_TextBox.Text = employee.LastName.Trim();
                FirstName_TextBox.Text = employee.LastName.Trim();
                Title_TextBox.Text = employee.Title.Trim();
                TitleOfCourtesy_TextBox.Text = employee.TitleOfCourtesy;
                BirthDate_TextBox.Text = (employee.BirthDate.ToString());
                HireDate_TextBox.Text = (employee.HireDate.ToString());
                Adress_TextBox.Text = employee.Address;
                City_TextBox.Text = employee.City;
                Region_TextBox.Text = employee.Region;
                PostalCode_TextBox.Text = employee.PostalCode;
                Country_TextBox.Text = employee.Country;
                HomePhone_TextBox.Text = employee.HomePhone;
                Extension_TextBox.Text = employee.Extension;
                Notes_TextBox.Text = employee.Notes;
                ReportsTo_TextBox.Text = employee.ReportsTo.ToString();
            }
            catch (FormatException ex)
            {
                Response.Write("Opps! Employeen finns inte! / felaktig data inmatad - ");
                Response.Write("Felmeddelande: ");
                Response.Write(ex.Message);
                Response.Write(" Börja om genom att klicka <a href='Default.aspx'>" +
                               "här.");
            }
            catch (NullReferenceException ex)
            {
                Response.Write("Opps! Employeen finns inte! - ");
                Response.Write("Felmeddelande: ");
                Response.Write(ex.Message);
                Response.Write(" Börja om genom att klicka <a href='Default.aspx'>" +
                               "här.");
            }
            catch (Exception ex)
            {
                Response.Write("Opps! något gick fel! - ");
                Response.Write("Felmeddelande: ");
                Response.Write(ex.Message);
                Response.Write(" Börja om genom att klicka <a href='Default.aspx'>" +
                    "här.");
            }
        }

        protected void SaveEmployee_Button_Click(object sender, EventArgs e)
        {

        }
    }
}