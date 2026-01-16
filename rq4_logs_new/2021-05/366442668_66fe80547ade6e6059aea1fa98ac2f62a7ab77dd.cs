using FinalProject_ADONET.EF;
using FinalProject_ADONET.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
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
using System.Windows.Threading;

namespace FinalProject_ADONET.Views
{
    /// <summary>
    /// Логика взаимодействия для AdministrationLoginWindow.xaml
    /// </summary>
    public partial class AdministrationLoginWindow : Window
    {
        DataManager _dataManager;
        MainWindow _mainWindow;
        bool succes;
        private DispatcherTimer timer = null;
        private int countTimer = 5;
        public string Login { get; set; }

        public AdministrationLoginWindow(MainWindow mainWindow)
        {
            InitializeComponent();
            _dataManager = new DataManager();
            _mainWindow = mainWindow;
            succes = false;
            LoginBox.Focus();
        }



        private void timer_Tick(object sender, EventArgs e)
        {
            if (succes)
            {
                statusEnter.Content = $"Успешно вход через : {countTimer}";
            }
            if (countTimer == 0) { timer.Stop(); this.Close(); };

            countTimer--;
        }
        private void enterButton_Click(object sender, RoutedEventArgs e)
        {
            if (!succes)
            {
                string login = LoginBox.Text;
                string password = GetMd5Hash(passwordBox.Password);

                var accounts = _dataManager.Accounts.ToList();
                foreach (Account a in accounts)
                {
                    if (a.Login == login && a.Passw == password)
                    {
                        succes = true;
                        Login = login;
                        break;
                    }
                }

                if (succes)
                {
                    statusEnter.Foreground = Brushes.Green;
                    statusEnter.Content = $"Успешно вход через : {countTimer}";

                    timer = new DispatcherTimer();
                    timer.Interval = new TimeSpan(0, 0, 0, 0, 250);
                    timer.Tick += new EventHandler(timer_Tick);
                    timer.Start();

                    _mainWindow.Close();
                }
                else
                {
                    statusEnter.Content = "Неверные данные!";
                }
                LoginBox.Clear();
                passwordBox.Clear();
            }
        }
        private string GetMd5Hash(string target)
        {
            MD5 encoder = MD5.Create();
            byte[] buff = Encoding.UTF8.GetBytes(target);
            byte[] data = encoder.ComputeHash(buff);

            StringBuilder sBuilder = new StringBuilder();
            for (int i = 0; i < data.Length; i++)
                sBuilder.Append(data[i].ToString("x2"));
            string result = sBuilder.ToString();

            return result;
        }
        private void Window_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            //if (!succes)
            //_mainWindow.Close();
        }
        private void LoginBox_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.Key == Key.Enter) passwordBox.Focus();
        }
        private void passwordBox_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.Key == Key.Enter) enterButton_Click(sender, e);
        }
    }
}