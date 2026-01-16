using MaterialDesignThemes.Wpf;
using Snake.Classes;
using Snake.Windows;
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
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace Snake.Pages
{
    public partial class BeforeStartPage : Page
    {
        private readonly GameMenu gameMenu;

        public BeforeStartPage(GameMenu gameMenu)
        {
            InitializeComponent();

            this.gameMenu = gameMenu;
        }

        private void BeforeStartPage_Loaded(object sender, RoutedEventArgs e)
        {
            //gameMenu.nickname = "Gabrielos";
            Nickname_WrapPanel.DataContext = gameMenu;
            Nickname_Text_Box.Focus();

            if (!gameMenu.MusicOn) Music_Icon.Kind = PackIconKind.MusicOff;
            else Music_Icon.Kind = PackIconKind.Music;
        }

        private void Nickname_TextChanged(object sender, TextChangedEventArgs e)
        {
            if (gameMenu.nickname == null || gameMenu.nickname.Length == 0)
            {
                Validation_TextBlock.Text = "Nickname can not be empty!";
                Validation_TextBlock.Visibility = Visibility.Visible;
                StartGame_Button.IsEnabled = false;
            }
            else if (gameMenu.nickname.Length < 3)
            {
                Validation_TextBlock.Text = "The nickname must consist of at least 3 characters";
                Validation_TextBlock.Visibility = Visibility.Visible;
                StartGame_Button.IsEnabled = false;
            }
            else
            {
                Validation_TextBlock.Visibility = Visibility.Hidden;
                StartGame_Button.IsEnabled = true;
            }
        }

        #region MOUSE ENTER/LEAVE BUTTONS
        private void Music_Button_MouseEnter(object sender, MouseEventArgs e)
        {
            if (gameMenu.MusicOn) Music_Button.ToolTip = "Turn off the music";
            else Music_Button.ToolTip = "Turn on the music";
        }

        private void NewGame_Button_MouseEnter(object sender, MouseEventArgs e)
        {
            StartGame_Button.Height = 37;
            StartGame_Button.Width = 225;

            StartGame_Button.Background = new SolidColorBrush(Color.FromRgb(30, 130, 35));
            StartGame_Button.BorderBrush = new SolidColorBrush(Color.FromRgb(30, 130, 35));
            StartGame_Button.Foreground = Brushes.White;
        }

        private void NewGame_Button_MouseLeave(object sender, MouseEventArgs e)
        {
            StartGame_Button.Height = 32;
            StartGame_Button.Width = 210;

            StartGame_Button.Background = new SolidColorBrush(Color.FromRgb(76, 175, 80));
            StartGame_Button.BorderBrush = new SolidColorBrush(Color.FromRgb(76, 175, 80));
            StartGame_Button.Foreground = Brushes.Black;
        }
        #endregion

        #region BUTTONS CLICKS
        private void BackButton_Click(object sender, RoutedEventArgs e)
        {
            NavigationService.Navigate(new MenuPage(gameMenu: gameMenu));
        }

        private void MusicButton_Click(object sender, RoutedEventArgs e)
        {
            if (gameMenu.MusicOn)
            {
                Music_Icon.Kind = PackIconKind.MusicOff;
                gameMenu.StopMusic();
                gameMenu.MusicOn = false;
            }
            else
            {
                Music_Icon.Kind = PackIconKind.Music;
                gameMenu.PlayMusic();
                gameMenu.MusicOn = true;
            }
        }

        private void ExitButton_Click(object sender, RoutedEventArgs e)
        {
            ExitWindow exitWindow = new ExitWindow { Owner = Window.GetWindow(this) };
            exitWindow.ShowDialog();
        }

        private void NewGameButton_Click(object sender, RoutedEventArgs e)
        {

        }
        #endregion
    }
}