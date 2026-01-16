// Copyright (C) 2020 Sascha Manns <Sascha.Manns@outlook.de>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

// Dependencies

using System;
using System.Windows;
using System.Windows.Controls;

namespace latex_curriculum_vitae
{
    /// <summary>
    /// Interaction logic for WriteJobApplication.xaml
    /// </summary>
    public partial class WriteJobApplicationWindow : Window
    {

        #region Initialize Main
        public WriteJobApplicationWindow()
        {
            InitializeComponent();
        }
        #endregion

        #region Generating job application
        private void BtnGenerate_Click(object sender, EventArgs e)
        {
            #region Private Variables
            string addressline;
            bool compemail_set;
            #endregion

            #region Cleanup
            Setup.Cleanup();
            #endregion

            if (txtJobtitle.Text == "" || txtCompanyName.Text == "")
            {
                BtnGenerate.Visibility = Visibility.Hidden;
            }
            else
            {
                BtnGenerate.Visibility = Visibility.Visible;
            }



            #region Settings
            User myuser = new User();
            #endregion

            #region JobApplication Data
            JobApplication myapplication = new JobApplication(txtURL.Text, txtEmail.Text, txtJobtitle.Text);

            if (myapplication.Email == "")
            {
                compemail_set = false;
            }
            else
            {
                compemail_set = true;
            }

            Company company;
            if (txtCompanyStreet.Text == "" || txtZIP.Text == "" || txtCity.Text == "")
            {
                company = new Company(txtCompanyName.Text);
            }
            else
            {
                company = new Company(txtCompanyName.Text, txtCompanyStreet.Text, Convert.ToInt32(txtZIP.Text), txtCity.Text);
            }

            ComboBoxItem typeItem = (ComboBoxItem)cboGender.SelectedItem;
            string gender = typeItem.Content.ToString();

            Contact contact = new Contact(txtContactName.Text, gender);
            addressline = contact.Addressline(company.Name, contact.Name, contact.Gender, company.Street, company.ZIP, company.City);
            #endregion

            #region Build, Compile and Send
            Build.PrepareBuild();
            string subject = Build.GetSubject(myuser.Subjectprefix, myapplication.Jobtitle);
            Build.CreateApplicationConfig(myapplication.Jobtitle, company.Name, contact.Name, company.Street, company.City, contact.Salutation, subject, addressline);
            Build.CompileApplication();
            Build.CombineApplication(myuser.Firstname, myuser.Familyname);

            if (compemail_set == false)
            {
                Build.OpenExplorer();

            }
            else
            {
                subject = Build.GetEmailSubject(myuser.Subjectprefix, myapplication.Jobtitle);
                string finalpdf = Build.GetFinalPdfName(myuser.Firstname, myuser.Familyname);
                Email.CreateMessage(myuser.Firstname, myuser.Familyname, myuser.Email, contact.Name, myapplication.Email, subject, contact.Salutation, finalpdf, myuser.SmtpServer, myuser.SmtpUser, myuser.SmtpPass, myuser.SmtpPort);
            }
            #endregion

            #region Add Information to CSV
            CSVExport.WriteCSV(company.Name, myapplication.Jobtitle, company.City, myapplication.URL);
            #endregion

            #region Clean UI
            txtCity.Clear();
            txtCompanyName.Clear();
            txtCompanyStreet.Clear();
            txtContactName.Clear();
            txtEmail.Clear();
            txtJobtitle.Clear();
            txtURL.Clear();
            txtZIP.Clear();
            #endregion
        }
        #endregion

        #region Exit App
        private void BtnExit_Click(object sender, EventArgs e)
        {
            this.Close();
        }
        #endregion

        #region Get Onlinehelp
        private void BtnGetOnlineHelp_Click(object sender, EventArgs e)
        {
            Setup.GetOnlineDocumentation();
        }
        #endregion        
    }
}