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
    /// Логика взаимодействия для ChangeWindow.xaml
    /// </summary>
    public partial class ChangeWindow : Window
    {
        public ChangeWindow()
        {
            InitializeComponent();
            DateTime a = DateTime.Now;
            string b = a.ToString("d");
            Console.WriteLine(b);
            int year = DateTime.Now.Year;
            int month = DateTime.Now.Month;
            int day = DateTime.Now.Day;

            int nextyear = year + 1;

            DateTime start = new DateTime(year, month, 1, 3, 0, 0); // год, месяц, число, день, час, минута.
            DateTime end = new DateTime(nextyear, month, day, 3, 0, 0);

            dp.DisplayDateStart = start;
            dp.DisplayDateEnd = end;
        }

        private void Button_Click(object sender, RoutedEventArgs e)
        {
            string a = dp.Text;
            string b = NameEvent_TB.Text;
            string c = DescEvent_TB.Text;
            if (a == "" || b == "" || c == "")
            {
                MessageBox.Show("Вы не заполнили какое-то из полей!");
                return;
            }
            try
            {
                SqlConnection con = new SqlConnection(@"Data Source=stud-mssql.sttec.yar.ru,38325;user id=user212_db;password=user212;Initial Catalog=user212_db;Trusted_Connection=False;");//Соединение
                con.Open();
                SqlCommand com1 = new SqlCommand($"INSERT INTO KALENDAR(Name, Description, Date) VALUES ('{b}','{c}','{a}')", con);
                com1.ExecuteNonQuery();
                con.Close();
                MessageBox.Show("Успешно!");
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
                return;
            }
        }
    }
}