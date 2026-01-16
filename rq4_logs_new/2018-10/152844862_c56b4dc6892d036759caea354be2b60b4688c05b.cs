namespace AirNewUI
﻿using AirNewUI;
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

namespace AirTicket
{
    /// <summary>
    /// 機票訂單.xaml 的互動邏輯
    /// </summary>
    public partial class 機票訂單
    {
        AirEntities DbContext = new AirEntities();

        CollectionViewSource orderViewSource;
        public 機票訂單()
        {
            InitializeComponent();
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            DbContext.Orders.AsEnumerable().ToList();
            orderViewSource = ((System.Windows.Data.CollectionViewSource)(this.FindResource("orderViewSource")));
            // 透過設定 CollectionViewSource.Source 屬性載入資料: 
            // orderViewSource.Source = [泛用資料來源]
            orderViewSource.Source = DbContext.Orders.Local;

            var result = from o in this.DbContext.Orders
                         join a in this.DbContext.Airlines on o.Airline_ID equals a.Airline_ID
                         join f in this.DbContext.Flights on o.Flight_ID equals f.Flight_ID
                         join ap in this.DbContext.Airports on o.Arrival_Airport_ID equals ap.Airport_ID
                         join c in this.DbContext.Cabins on o.Cabin_ID equals c.Cabin_ID
                         //join p in this.DbContext.Passengers on o.Ticket_ID equals p.Ticket_ID                         
                         select new { 訂單編號 = o.Ticket_ID, 航空公司 = o.Airline.Airline_Name, 出發地 = o.Airport.Airport_Name, 目的地 = o.Airport1.Airport_Name, 艙等 = o.Cabin.Cabin_Name, 出發時間 = o.Departure_Time, 抵達時間 = o.Arrival_Time, 直飛或轉機 = o.NonStop,會員編號=o.Member_ID, 訂單日期 = o.Order_Time, 付款方式 = o.Payment/* ,乘客=p.Order.Ticket_ID*/ };
            this.DataGrid1.ItemsSource =result.ToList();
            
        }

        private void Button_Click(object sender, RoutedEventArgs e)
        {
            //todo 日期搜尋
            
            //DateTime Departure_Date = this.DatePicker1.SelectedDate.Value;
            string AirLineName = this.AirLineName.Text;

            //if (this.AirLineName.Text is null && this.DatePicker1.SelectedDate == null)
            //{
            //    MessageBox.Show("請輸入查詢航空公司或日期");
            //}
            //else if (this.AirLineName.Text == null && this.DatePicker1.SelectedDate != null)
            //{
            //    //查詢日期                
            //    this.orderViewSource.Source = this.DbContext.Orders.Where(o => o.Departure_Time == Departure_Date).ToList();

            //}
            //else if (this.AirLineName.Text != null && this.DatePicker1.SelectedDate == null)
            //{
            //    //查詢航空公司
            //    this.orderViewSource.Source = this.DbContext.Orders.Where(o => o.Airline.Airline_Name == AirLineName).ToList();
            //}
            //else
            //{
            //    this.orderViewSource.Source = this.DbContext.Orders.Where(o => o.Airline.Airline_Name == AirLineName && o.Departure_Time == Departure_Date).ToList();
            //}
            
            var result = from o in this.DbContext.Orders
                         join a in this.DbContext.Airlines on o.Airline_ID equals a.Airline_ID
                         join f in this.DbContext.Flights on o.Flight_ID equals f.Flight_ID
                         join ap in this.DbContext.Airports on o.Arrival_Airport_ID equals ap.Airport_ID
                         join c in this.DbContext.Cabins on o.Cabin_ID equals c.Cabin_ID
                         //where o.Departure_Time.Value >= Departure_Date 
                         where o.Airline.Airline_Name == AirLineName
                         select new { 訂單編號 = o.Ticket_ID, 航空公司 = o.Airline.Airline_Name, 出發地 = o.Airport.Airport_Name, 目的地 = o.Airport1.Airport_Name, 艙等 = o.Cabin.Cabin_Name, 出發時間 = o.Departure_Time, 抵達時間 = o.Arrival_Time, 直飛或轉機 = o.NonStop, 會員編號 = o.Member_ID, 訂單日期 = o.Order_Time, 付款方式 = o.Payment/* ,乘客=p.Order.Ticket_ID*/ };
            this.DataGrid1.ItemsSource = result.ToList();

        }
    }
}
﻿using System;
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
using System.Data.Entity;

namespace AirTicket
{
    /// <summary>
    /// 搜尋機票.xaml 的互動邏輯
    /// </summary>
    
    public partial class 搜尋機票 : Window
    {
        public int num;
        public string flight;
        public string departure_airport_name;
        public string time1;
        public string duration;
        public string arrival_airport_name;
        public string time2;
        public string cabin;
        public string price;

        public 搜尋機票()
        {
            InitializeComponent();
            // 來回的回程日期 預設隱藏
            this.DatePicker2.Visibility = Visibility.Hidden;

            //把資料庫機場資料繫結到兩個combobox
            var data = from p in this.DbContext.Airports
                       select p.Airport_Name;
            this.combobox1.ItemsSource = data.ToList();
            this.combobox2.ItemsSource = data.ToList();
        }
        AirEntities1 DbContext = new AirEntities1();
        //CollectionViewSource FilghtViewSource;
        private void Button_Click(object sender, RoutedEventArgs e)
        {

            num = Int32.Parse( this.comboboxnum.Text);
            this.stackpanel1.Children.Clear();
            string Departure_Airport = this.combobox1.Text;
            string Arrival_Airport = this.combobox2.Text;
            string Departure_Date = this.DatePicker1.SelectedDate.Value.ToShortDateString();
            // string Arrival_Date = this.DatePicker2.SelectedDate.Value.ToShortDateString();


            var date = DateTime.Parse(Departure_Date);
            //var date2= DateTime.Parse(Arrival_Date);


           

            var q = from a in this.DbContext.Airlines
                    join apl in this.DbContext.Airplains on a.Airline_ID equals apl.Airline_ID
                    join f in this.DbContext.Flights on apl.Airplain_ID equals f.Airplain_ID
                    join ap in this.DbContext.Airports on f.Airport_ID equals ap.Airport_ID
                    join acd in this.DbContext.AirplainCabinDetails on f.Flight_ID equals acd.Flight_ID
                    join c in this.DbContext.Cabins on acd.Cabin_ID equals c.Cabin_ID
                    where ap.Airport_Name == Arrival_Airport && f.Departure_Date >= date && f.Departure_Date <= DbFunctions.AddDays(date, 1)
                    select new
                    {
                        Flight_ID = f.Flight_ID,
                        Airport_Name = ap.Airport_Name,
                        Departure_Date = f.Departure_Date,
                        Arrival_Date = f.Arrival_date,
                        Duration = f.Duration,
                        Airline_Name = a.Airline_Name,
                        Price = acd.Price,
                        Cabin_Name=c.Cabin_Name
                    };






            //把q的資料一筆一筆放到使用者控制項中
            foreach (var q1 in q)
            {
                UserControlFlight ucf = new UserControlFlight();
                this.stackpanel1.Children.Add(ucf);
                ucf.label1.Content = q1.Airline_Name;
                ucf.Departure_Airport_lab.Content = "台北桃園(TPE)";
                ucf.Time1.Content = q1.Departure_Date;
                ucf.Duration.Content = q1.Duration;
                ucf.Arrival_Airport_lab.Content = q1.Airport_Name;
                ucf.Time2.Content = q1.Arrival_Date;
                //
                ucf.Tag = q1.Flight_ID;
                
                //把頭等艙跟經濟艙價錢放入button
                foreach (var q2 in q)
                {
                    if (q1.Airline_Name == q2.Airline_Name && q1.Departure_Date==q2.Departure_Date && q1.Duration==q2.Duration && q1.Airport_Name==q2.Airport_Name && q1.Arrival_Date==q2.Arrival_Date)
                    {
                        if (ucf.Cabin1.Content.ToString() == q2.Cabin_Name)
                        {
                            ucf.button1.Content = "NT "+q2.Price+ "/位";
                        }
                        else if (ucf.Cabin2.Content.ToString() == q2.Cabin_Name)
                        {
                            ucf.button2.Content = "NT " + q2.Price + "/位";
                        }
                    }

                }

                ucf.AddChild1 += Ucf_AddChild1;
                ucf.AddChild2 += Ucf_AddChild2;

            }

        }
        //點擊使用者控制項的經濟艙按鈕
        private void Ucf_AddChild2(object sender, EventArgs e)
        {
            string str = ((UserControlFlight)(sender)).button2.Content.ToString();    //我們抓取當前字串當中的數字
            string result = System.Text.RegularExpressions.Regex.Replace(str, @"[^0-9]+", "");

            //抓取點擊按鈕中使用者控制項所有內容
            flight = ((UserControlFlight)(sender)).label1.Content.ToString();
            departure_airport_name = ((UserControlFlight)(sender)).Departure_Airport_lab.Content.ToString();
            time1 = ((UserControlFlight)(sender)).Time1.Content.ToString();
            duration = ((UserControlFlight)(sender)).Duration.Content.ToString();
            arrival_airport_name = ((UserControlFlight)(sender)).Arrival_Airport_lab.Content.ToString();
            time2 = ((UserControlFlight)(sender)).Time2.Content.ToString();
            cabin = "經濟艙";
            price = result;
            //控制項上的航班ID
            Tag = (int)((UserControlFlight)(sender)).Tag;
            

            MessageBox.Show(flight + " " + departure_airport_name + " " + time1 + " " + duration + " " + arrival_airport_name + " " + time2 + " " + cabin + " " + price);

            
            旅客資料填寫 f = new 旅客資料填寫();
            f.getTicket = this;
            f.Show();

          

            
        }
        //點擊使用者控制項的頭等艙按鈕
        private void Ucf_AddChild1(object sender, EventArgs e)
        {
            string str = ((UserControlFlight)(sender)).button1.Content.ToString();    //我們抓取當前字串當中的數字
            string result = System.Text.RegularExpressions.Regex.Replace(str, @"[^0-9]+", "");

            flight = ((UserControlFlight)(sender)).label1.Content.ToString();
            departure_airport_name = ((UserControlFlight)(sender)).Departure_Airport_lab.Content.ToString();
            time1 = ((UserControlFlight)(sender)).Time1.Content.ToString();
            duration = ((UserControlFlight)(sender)).Duration.Content.ToString();
            arrival_airport_name = ((UserControlFlight)(sender)).Arrival_Airport_lab.Content.ToString();
            time2 = ((UserControlFlight)(sender)).Time2.Content.ToString();
            cabin = "頭等艙";
            price = result;
            //控制項上的航班ID
            Tag = (int)((UserControlFlight)(sender)).Tag;

            MessageBox.Show(flight + " " + departure_airport_name + " " + time1 + " " + duration + " " + arrival_airport_name + " " + time2 + " " + cabin + " " + price);

            旅客資料填寫 f = new 旅客資料填寫();
            f.getTicket = this;
            f.Show();
        }

        private void ComboBox_KeyUp(object sender, KeyEventArgs e)
        {
            ComboBox cmb = (ComboBox)sender;
            CollectionView itemsViewOriginal = (CollectionView)CollectionViewSource.GetDefaultView(cmb.ItemsSource);

            itemsViewOriginal.Filter = ((o) =>
            {
                if (string.IsNullOrEmpty(cmb.Text)) return false;
                else
                {
                    if (((string)o).StartsWith(cmb.Text)) return true;
                    else return false;
                }
            });

            cmb.IsDropDownOpen = true;
            itemsViewOriginal.Refresh();

        }

        private void combobox2_KeyUp(object sender, KeyEventArgs e)
        {
            ComboBox cmb = (ComboBox)sender;
            CollectionView itemsViewOriginal = (CollectionView)CollectionViewSource.GetDefaultView(cmb.ItemsSource);

            itemsViewOriginal.Filter = ((o) =>
            {
                if (string.IsNullOrEmpty(cmb.Text)) return false;
                else
                {
                    if (((string)o).StartsWith(cmb.Text)) return true;
                    else return false;
                }
            });

            cmb.IsDropDownOpen = true;
            itemsViewOriginal.Refresh();
        }

        private void DatePicker1_SelectedDateChanged(object sender, SelectionChangedEventArgs e)
        {
            //當第一個日期選定時  第二個日期會從第一個日期開始
            this.DatePicker2.DisplayDateStart = this.DatePicker1.SelectedDate.Value;
        }

        private void RadioButton_Checked(object sender, RoutedEventArgs e)
        {
            //點選來回選項會出現回程時間
            this.DatePicker2.Visibility = Visibility.Visible;
        }

        private void RadioButton_Checked_1(object sender, RoutedEventArgs e)
        {
            //點選單程把回程時間隱藏
            this.DatePicker2.Visibility = Visibility.Hidden;
        }
    }
    
    
}