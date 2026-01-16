using System;
using System.Collections.Generic;
using System.Data.SqlClient;
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

namespace KALENDARV2
{
    /// <summary>
    /// Логика взаимодействия для WatchWindow.xaml
    /// </summary>
    public partial class WatchWindow : Window
    {
        public WatchWindow()
        {
            InitializeComponent();
        }

        private Button CreateBT(string id, string name, string description, int j, int i)
        {
            Button bt = new Button();
            bt.Width = 100;
            bt.Height = 100;
            bt.FontSize = 20;
            bt.VerticalAlignment = VerticalAlignment.Top;
            bt.HorizontalAlignment = HorizontalAlignment.Left;
            bt.Margin = new Thickness(i * 110, 110 + (j * 110), 0, 0);
            bt.Name = "BT" + id;
            bt.Click += Bt_Click;
            bt.HorizontalContentAlignment = HorizontalAlignment.Center;
            bt.VerticalContentAlignment = VerticalAlignment.Center;
            bt.Content = name + Environment.NewLine + description;
            return bt;
        }

        private void Bt_Click(object sender, RoutedEventArgs e)
        {
            Button bt = (Button)sender;
            string name = bt.Name;
            name = name.Remove(0, 2);
            if (name != "")
            {
                SqlConnection con = new SqlConnection(@"Data Source=stud-mssql.sttec.yar.ru,38325;user id=user212_db;password=user212;Initial Catalog=user212_db;Trusted_Connection=False;");//Соединение
                con.Open();
                SqlCommand cmd = new SqlCommand("SELECT Name FROM KALENDAR WHERE ID = " + name + ";", con);
                SqlCommand cmd1 = new SqlCommand("SELECT Description FROM KALENDAR WHERE ID = " + name + ";", con);
                string a = Convert.ToString(cmd.ExecuteScalar());
                string b = Convert.ToString(cmd1.ExecuteScalar());
                con.Close();
                MessageBox.Show(a + Environment.NewLine + b);
            }
        }

        private void FillYearCB()
        {
            int year = DateTime.Now.Year;
            YearCB.Items.Add(year);
            YearCB.Items.Add(year + 1);
        }


        private void FillWindow()
        {
            Gd.Children.Clear();
            int hm = Convert.ToInt32(DateTime.DaysInMonth((int)YearCB.SelectedValue, 1 + (int)MonthCB.SelectedIndex));
            int j = 0;
            int h = 1;

            int k = ((int)Var.dt) - 1;
            for (int i = 1 + (int)Var.dt; i <= (hm + (int)Var.dt); i++)
            {
                if (k % 7 == 0 && i != (1 + (int)Var.dt))
                {
                    j++;
                    k = 0;
                }
                k++;
                if (k == 0)
                {
                    k = 7;
                }
                string year = Convert.ToString(YearCB.SelectedValue);
                string month = Convert.ToString((MonthCB.SelectedIndex) + 1);
                string day = Convert.ToString(h);
                string date = day + "." + month + "." + year;
                DateTime nd = Convert.ToDateTime(date);
                date = nd.ToShortDateString();
                string desc = "";
                SqlConnection con = new SqlConnection(@"Data Source=stud-mssql.sttec.yar.ru,38325;user id=user212_db;password=user212;Initial Catalog=user212_db;Trusted_Connection=False;");//Соединение
                con.Open();
                SqlCommand cmd = new SqlCommand("SELECT Name FROM KALENDAR WHERE Date = '" + date + "';", con);
                SqlCommand cmd1 = new SqlCommand("SELECT ID FROM KALENDAR WHERE Date = '" + date + "';", con);
                string id = Convert.ToString(cmd1.ExecuteScalar());
                desc = Convert.ToString(cmd.ExecuteScalar());
                con.Close();
                Gd.Children.Add(CreateBT(id, day, desc, j, k));
                h++;
            }
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            FillYearCB();
            YearCB.SelectedIndex = 0;
            MonthCB.SelectedIndex = Convert.ToInt32(DateTime.Now.Month) - 1;
        }

        private void YearCB_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            try
            {
                int month = (int)MonthCB.SelectedIndex + 1;
                Var.dt = (int)new DateTime((int)YearCB.SelectedValue, month, 1).DayOfWeek;
                FillWindow();
            }
            catch
            {
                Console.WriteLine("Подгрузочка");
            }
        }

        private void MonthCB_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            try
            {
                int month = (int)MonthCB.SelectedIndex + 1;
                Var.dt = (int)new DateTime((int)YearCB.SelectedValue, month, 1).DayOfWeek;
                FillWindow();
            }
            catch
            {
                Console.WriteLine("Подгрузочка");
            }
        }
    }
}
