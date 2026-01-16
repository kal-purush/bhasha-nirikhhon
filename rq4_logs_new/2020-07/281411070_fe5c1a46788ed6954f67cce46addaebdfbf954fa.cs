using System.Linq;
using System.Windows;
using TwipsterApp.Models;

namespace TwipsterApp
{
    /// <summary>
    /// Interaction logic for TwipsterMainWindow.xaml
    /// </summary>
    public partial class TwipsterMainWindow : Window
    {
        public TwipsterMainWindow()
        {
            InitializeComponent();
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            var currentUser = LoginWindow.currentUser;
            User[] users;

            using (var context = new TwipsterDbContext())
            {
                users = context.Users.OrderBy(x => x.Name).ToArray();
            };
            //Deleting current user and users passwords and logins from array
            var usersCensored = users.Where(x => x.Login != currentUser.Login)
                                     .Select(x => new { x.Name, x.Surname, x.BirthDate });

            CurrentUserInfoToTextBox(currentUser);
            UsersGrid.ItemsSource = usersCensored.ToList();
        }

        private void LogOutButton_Click(object sender, RoutedEventArgs e)
        {
            var loginWindow = new LoginWindow();
            loginWindow.Show();
            Close();
        }

        private void CurrentUserInfoToTextBox(User currentUser)
        {
            CurrentUserTextBlock.Text = $"{currentUser.Name} {currentUser.Surname} \nDate of birth: {currentUser.BirthDate.Date.ToString()} \nLogin: {currentUser.Login}";
        }
    }
}