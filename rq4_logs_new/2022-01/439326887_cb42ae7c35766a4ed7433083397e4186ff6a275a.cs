using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
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

namespace _1Semprojekt2022_Golf
{
    /// <summary>
    /// Interaction logic for Create_runner_finishtime.xaml
    /// </summary>
    public partial class Create_runner_finishtime : Window
    {

        public ObservableCollection<Person> People { get; set; }
        public Create_runner_finishtime(Administrator admin)
        {

            InitializeComponent(); 
            people_data();
            test();
            this.Closing += new System.ComponentModel.CancelEventHandler(Create_runner_finishtime_Closing);




        }



            private void Button_Click(object sender, RoutedEventArgs e)
            {
                var selectedOnes = People.Where(x => x.Include == true).ToList();
                foreach (Person person in selectedOnes)
                {
                    Debug.WriteLine(person.Name);
                }
            }


        void test()
        {


            String Minute = DateTime.Now.ToString("mm");
            String houre = DateTime.Now.ToString("HH");
            String seconds = DateTime.Now.ToString("ss");
            Test_text_1.Text = Minute;
            Test_text_2.Text = houre;
            Test_text_3.Text = seconds;

        }


        void people_data()
        {
            People = new ObservableCollection<Person>
            {
                new Person {Id=1, Name="a" },
                new Person {Id=2, Name="b" },
                new Person {Id=3, Name="c" },
                new Person {Id=4, Name="d" },
                new Person {Id=5, Name="e" }
            };
        }

        void Create_runner_finishtime_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            MainWindow secondWindow = new MainWindow();
            secondWindow.Show();
        }



    }
}