using PartnersMatcher.Model.Domains;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;

namespace PartnersMatcher
{
    /// <summary>
    /// Interaction logic for EditUserProfile.xaml
    /// </summary>
    public partial class EditUserProfile : Window
    {
        string name;
        string age;
        string phone;
        string city;
        string mail;
        string text_changed; //to know if "other" field is empty or not
        public static List<IDomain> l;
        List<string> domain_name;

        public EditUserProfile(string _mail)
        {
            InitializeComponent();
            name = "";
            age = "";
            phone = "";
            city = "";
            text_changed = "";
            mail = _mail;
            l = new List<IDomain>();
            domain_name = new List<string>();
        }

        private void Age_TextChanged(object sender, TextChangedEventArgs e)
        {

        }

        private void Phone_TextChanged(object sender, TextChangedEventArgs e)
        {

        }

        private void UserFullName_TextChanged(object sender, TextChangedEventArgs e)
        {
            
        }

        private void City_TextChanged(object sender, TextChangedEventArgs e)
        {

        }

        private void create(object sender, RoutedEventArgs e)
        {
            name = this.UserFullNametextBox.Text;               
            age = this.AgetextBox.Text;
            phone = this.PhonetextBox.Text;
            city = this.CitytextBox.Text;
            if(name=="" || age=="" || phone=="" || city=="")
                MessageBox.Show("One or more fields are missing", "OK");
            else if ((Double.Parse(age)<1 || Double.Parse(age)>100))
                MessageBox.Show("Age is incorrect", "OK");
            else
            {
                User u = new User(mail, name, age, phone, city);
                //add domain to user
                foreach(IDomain d in l)
                {
                    u.domains.Add(d);
                }
                //add domain name to user
                foreach(string s in domain_name)
                {
                    u.domain_name.Add(s);
                }
                //add user
                MainWindow.all_users.Add(mail, u);
                MessageBox.Show("User had been added succefully", "OK");
                if(text_changed!="")
                    MessageBox.Show("Your request for new domain sended to system manager confirmation", "OK");
                registeredUserView r = new registeredUserView();
                r.Show();
                this.Close();
            }            
        }

        private void _exit(object sender, RoutedEventArgs e)
        {
            this.Close();
        }

        private void date_Checked(object sender, RoutedEventArgs e)
        {

        }

        private void date_Unchecked(object sender, RoutedEventArgs e)
        {

        }

        private void trip_Checked(object sender, RoutedEventArgs e)
        {

        }

        private void trip_Unchecked(object sender, RoutedEventArgs e)
        {

        }

        private void acomodation_Checked(object sender, RoutedEventArgs e)
        {
            domain_name.Add("Accomodation");
            acomodation a = new acomodation();
            a.Show();
        }

        private void acomodation_Unchecked(object sender, RoutedEventArgs e)
        {
            domain_name.Remove("Accomodation");
        }

        private void sport_Checked(object sender, RoutedEventArgs e)
        {

        }

        private void sport_Unchecked(object sender, RoutedEventArgs e)
        {

        }

        private void OtherD_TextChanged(object sender, TextChangedEventArgs e)
        {
            text_changed = this.OtherDtextBox.Text;
        }
    }
}