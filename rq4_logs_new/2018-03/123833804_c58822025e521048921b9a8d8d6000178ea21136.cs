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

namespace FileEncryptionTool
{
    /// <summary>
    /// Interaction logic for Recipients_Window.xaml
    /// </summary>
    public partial class Recipients_Window : Window
    {

        private ListBox _recipients;
       
        public Recipients_Window(ListBox recipients) 
        {
            this.WindowStartupLocation = WindowStartupLocation.CenterScreen;
            InitializeComponent();
            listBox.SelectionMode = SelectionMode.Multiple;
            listBox.ItemsSource = User.loadUsers();
            _recipients = recipients;
        }


        private void button_Click(object sender, RoutedEventArgs e)
        {
            foreach (User item in listBox.SelectedItems)
            {
                if (!_recipients.Items.Cast<User>().Contains(item))
                {
                    _recipients.Items.Add(item);
                }

                this.Close();
            }
        }
    }
}