using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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
using System.Xml.Linq;

namespace FileEncryptionTool
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        private string _algorithmName;
        private int _keySize;
        private int _blockSize;
        private string _cipherModeName;
        private string _ivName;

        private List<User> _users;

        private class User
        {
            public User(string email, string sessionKey)
            {
                _email = email;
                _sessionKey = sessionKey;
            }

            public string _email;
            public string _sessionKey; //TODO change type
        }

        public MainWindow()
        {
            InitializeComponent();
        }

        private void Button_Click(object sender, RoutedEventArgs e)
        {
            OpenFileDialog openFileDialog = new OpenFileDialog();
            if (openFileDialog.ShowDialog() == true)
                outputFileName.Text = openFileDialog.FileName;
        }

        private void TextBox_TextChanged(object sender, TextChangedEventArgs e)
        {

        }

        private void Button_Click_1(object sender, RoutedEventArgs e)
        {
            RNG_Window win2 = new RNG_Window(Update_RNG);
            win2.ShowDialog();
        }

        private void Update_RNG(List<Point> coords)
        {
            StringBuilder sb = new StringBuilder();
            foreach (var p in coords)
            {
                sb.Append(p.ToString() + "\n");
            }
            sb.Append(System.DateTime.Now.ToBinary().ToString() + "\n");

            using (var uptime = new PerformanceCounter("System", "System Up Time"))
            {
                uptime.NextValue();       //Call this an extra time before reading its value
                sb.Append(TimeSpan.FromSeconds(uptime.NextValue()).ToString());
            }

            rng_result.Text = sb.ToString();
        }

        private void Button_Click_2(object sender, RoutedEventArgs e)
        {
            _algorithmName = "algoName";
            _keySize = 32;
            _blockSize = 128;
            _cipherModeName = "cipherName";
            _ivName = "ivName";
            _users = new List<User>();
            _users.Add(new User("user@gmail.com", "123123123123123"));


            //TODO add error checking for block below
            XDocument xdoc = new XDocument(
                new XElement("EncryptedFileHeader",
                    new XElement("Algorightm", _algorithmName),
                    new XElement("KeySize", _keySize.ToString()),
                    new XElement("BlockSize", _blockSize.ToString()),
                    new XElement("CipherMode", _cipherModeName),
                    new XElement("IV", _ivName),
                    new XElement("ApprovedUsers",
                        from user in _users
                        select new XElement("User",
                            new XElement("Email", user._email),
                            new XElement("SessionKey", user._sessionKey)
                        )
                    )
                )
            );
            //TODO add creating directory if doesn't exist
            string directory = (Directory.Exists(outputFileName.Text) ? outputFileName.Text : @"C:\test\default.xml");
            using (StreamWriter writer = new StreamWriter(directory, false))
            {
                xdoc.Save(writer);
                writer.Write(rng_result.Text);
                MessageBox.Show("Pomyślnie zapisano do pliku");
            }

        }
    }
}