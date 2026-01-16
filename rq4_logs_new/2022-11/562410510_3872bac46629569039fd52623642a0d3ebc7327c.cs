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

namespace WPFDemo
{
    /// <summary>
    /// Interaction logic for Window2.xaml
    /// </summary>
    public partial class Window2 : Window
    {
        public Window2()
        {
            InitializeComponent();
        }

        private void Button_Click(object sender, RoutedEventArgs e)
        {
                List<string> colors = new List<string>();
                //add items in a List collection
                colors.Add("Red");
                colors.Add("Blue");
                colors.Add("Green");
                //insert an item in the list
                colors.Insert(1, "violet");
                //retrieve items using foreach loop
                foreach (string color in colors)
                {
                    MessageBox.Show(color);
                }
                //remove an item from list
                colors.Remove("violet");
                //retrieve items using for loop
                for (int i = 0; i < colors.Count; i++)
                {
                    MessageBox.Show(colors[i]);
                }
                if (colors.Contains("Blue"))
                {
                    MessageBox.Show("Blue color exists in the list");
                }
                else
                {
                    MessageBox.Show("Doesn't exist");
                }
                //copy array to list
                string[] strArr = new string[3];
                strArr[0] = "Red";
                strArr[1] = "Blue";
                strArr[2] = "Green";
                List<string> arrlist = new List<string>(strArr);
                foreach (string str in strArr)
                {
                    MessageBox.Show(str);
                }
                //call clear method
                arrlist.Clear();
                MessageBox.Show(arrlist.Count.ToString());
        }

        private void CloseBtn_Click(object sender, RoutedEventArgs e)
        {
            MainWindow mainWindow = new MainWindow();
            this.Close();
            mainWindow.Show();  
        }
    }
}