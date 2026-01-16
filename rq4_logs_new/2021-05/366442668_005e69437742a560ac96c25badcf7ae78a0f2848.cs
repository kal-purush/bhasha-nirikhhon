using FinalProject_ADONET.EF;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;

namespace FinalProject_ADONET.Views
{
    public partial class RegistrationWindow : Window
    {
        DataManager dm = new DataManager();

        bool FioStatus = false;
        bool PhoneStatus = false;
        bool EmailStatus = false;
        bool LoginStatus = false;
        bool PasswStatus = false;

        public RegistrationWindow()
        {
            InitializeComponent();
        }

        private void FIO_TextChanged(object sender, TextChangedEventArgs e)
        {
            
            if (!String.IsNullOrWhiteSpace(FIO.Text))
            {
                if (FIO.Text.Contains(" ") && FIO.Text.Length > 5) 
                {
                    foreach(var i in FIO.Text)
                    {
                        if (Char.IsLetter(i) || i.ToString() == " ")
                        {
                            FioStatus = true;
                        }
                        else { FioStatus = false; break; }
                    }
                    if (FioStatus)
                    { fioStatus.Content = "✅"; fioStatus.Foreground = Brushes.Green; }

                    else
                    {
                        fioStatus.Content = "❌";
                        fioStatus.Foreground = Brushes.Red;
                        PhoneStatus = false;
                    }
                }
                else
                {
                    fioStatus.Content = "❌";
                    fioStatus.Foreground = Brushes.Red;
                    PhoneStatus = false;
                }

            }
            else
            {
                fioStatus.Content = "❌";
                fioStatus.Foreground = Brushes.Red;
            }


        }

        private void phone_TextChanged(object sender, TextChangedEventArgs e)
        {
            if (!String.IsNullOrWhiteSpace(phone.Text))
            {
                foreach(var i in phone.Text)
                {
                    if (Char.IsDigit(i))
                    {
                        PhoneStatus = true;
                    }
                    else { PhoneStatus = false; break; }
                }
                if(PhoneStatus == true && phone.Text.Length == 12)
                {
                    phoneStatus.Content = "✅"; phoneStatus.Foreground = Brushes.Green;
                }
                else
                {
                    phoneStatus.Content = "❌";
                    phoneStatus.Foreground = Brushes.Red;
                    PhoneStatus = false;
                }
            }
            else
            {
                phoneStatus.Content = "❌";
                phoneStatus.Foreground = Brushes.Red;
                PhoneStatus = false;
            }
        }

        private void email_TextChanged(object sender, TextChangedEventArgs e)
        {
            if (!String.IsNullOrWhiteSpace(email.Text))
            {
                if (email.Text.Contains("@gmail.com") || email.Text.Contains("@yahoo.com")
                    || email.Text.Contains("@ukr.net") || email.Text.Contains("@outlook.com"))
                    EmailStatus = true;
                if (EmailStatus == true && email.Text.Length > 5)
                {
                    emailStatus.Content = "✅"; emailStatus.Foreground = Brushes.Green;
                }
                else
                {
                    emailStatus.Content = "❌";
                    emailStatus.Foreground = Brushes.Red;
                    EmailStatus = false;
                }
            }
            else
            {
                emailStatus.Content = "❌";
                emailStatus.Foreground = Brushes.Red;
                EmailStatus = false;
            }
        }

        private void login_TextChanged(object sender, TextChangedEventArgs e)
        {
            if (!String.IsNullOrWhiteSpace(login.Text))
            {
                if(Regex.IsMatch(login.Text, @"^[a-zA-Z0-9]+$") && login.Text.Length > 5)
                {
                    LoginStatus = true;
                    loginStatus.Content = "✅"; loginStatus.Foreground = Brushes.Green;
                }
            }
            else
            {
                loginStatus.Content = "❌";
                loginStatus.Foreground = Brushes.Red;
                LoginStatus = false;
            }
        }

        private void passw_PasswordChanged(object sender, RoutedEventArgs e)
        {
            if (!String.IsNullOrWhiteSpace(passw.Password))
            {
                if (Regex.IsMatch(passw.Password, @"^[a-zA-Z0-9_]+$") && passw.Password.Length >= 8)
                {
                    PasswStatus = true;
                    passwStatus.Content = "✅"; passwStatus.Foreground = Brushes.Green;
                }
                else
                {
                    passwStatus.Content = "❌";
                    passwStatus.Foreground = Brushes.Red;
                    PasswStatus = false;
                }
            }
            else
            {
                passwStatus.Content = "❌";
                passwStatus.Foreground = Brushes.Red;
                PasswStatus = false;
            }
        }

        private void registrationEnterButton_Click(object sender, RoutedEventArgs e)
        {
            // нужно обновить класс Account
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
    }
}