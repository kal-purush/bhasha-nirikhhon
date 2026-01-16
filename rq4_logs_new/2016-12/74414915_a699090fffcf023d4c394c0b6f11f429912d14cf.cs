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

namespace BizWorks
{
    /// <summary>
    /// Interaction logic for ProfileTypeModify.xaml
    /// </summary>
    public partial class ProfileTypeModify : Window
    {
        private string sessionUserProfileSubmit;
        private string passedUsername;
        private string passedPosition;
        private int ID;

        public ProfileTypeModify(string sessionUser, string sentUsername, string position)
        {
            InitializeComponent();
            sessionUserProfileSubmit = sessionUser;
            passedUsername = sentUsername;
            passedPosition = position;

            Username.Content = passedUsername;
            PositionName.Content = passedPosition;
            PositionID.Content = UserPositionIDCollection.GetID(passedPosition).ToString();
            ID = UserPositionIDCollection.GetID(passedPosition);
            PositionPay.Content = UserPositionIDCollection.GetPay(passedPosition).ToString();
        }

        public void TextBox_GotFocus(object sender, RoutedEventArgs e)
        {
            TextBox tb = (TextBox)sender;
            tb.Text = string.Empty;
            tb.GotFocus -= TextBox_GotFocus;
        }

        private void Next_Click(object sender, RoutedEventArgs e)
        {
            UserEmployeeCollection.ModifyPosition(passedUsername, ID);
            MainMenu main = new MainMenu(sessionUserProfileSubmit);
            App.Current.MainWindow = main;
            this.Close();
            main.Show();
        }
    }
}