using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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

namespace GUI
{
    /// <summary>
    /// Interaction logic for Profile.xaml
    /// </summary>
    public partial class Profile : Window
    {
        public ObservableCollection<string> Profiles { get; set; }

        public Profile()
        {
            InitializeComponent();
            Profiles = new ObservableCollection<string>();
            ProfilesListBox.ItemsSource = Profiles;
        }

        private void CreateProfileButton_Click(object sender, RoutedEventArgs e)
        {
            string profilename = ProfileNameTextBox.Text;
            if (!string.IsNullOrWhiteSpace(profilename) && profilename != "Enter profile name" && !Profiles.Contains(profilename))
            {
                Profiles.Add(profilename);
                ProfileNameTextBox.Clear();
                ProfileNameTextBox.Text = "Enter profile name";
                ProfileNameTextBox.Foreground = System.Windows.Media.Brushes.Gray;
            }
            else
            {
                MessageBox.Show("Profile name cannot be empty, default, or duplicate.", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void ExitButton_Click(object sender, RoutedEventArgs e)
        {
            this.Close();
        }

        private void ProfileNameTextBox_GotFocus(object sender, RoutedEventArgs e)
        {
            if (ProfileNameTextBox.Text == "Enter profile name")
            {
                ProfileNameTextBox.Text = "";
                ProfileNameTextBox.Foreground = System.Windows.Media.Brushes.Black;
            }
        }

        private void ProfileNameTextBox_LostFocus(object sender, RoutedEventArgs e)
        {
            if (string.IsNullOrWhiteSpace(ProfileNameTextBox.Text))
            {
                ProfileNameTextBox.Text = "Enter profile name";
                ProfileNameTextBox.Foreground = System.Windows.Media.Brushes.Gray;
            }
        }
    }
}