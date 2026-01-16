using System;
using System.Collections.Generic;
using System.ComponentModel;
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

namespace Cal_App
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public class FrameworkElement : UIElement, INotifyPropertyChanged
    {
        private string resizeMode = "CanResizeWithGrip";
        public static readonly DependencyProperty YearProperty;
        private void OnPropertyChanged(string propertyName)
        {
            if (this.PropertyChanged != null)
            {
                this.PropertyChanged(this, new PropertyChangedEventArgs(propertyName));
            }
        }
        public event PropertyChangedEventHandler PropertyChanged;

        public int Year
        {
            get
            {
                return (int)GetValue(YearProperty);
            }

            set
            {
                SetValue(YearProperty, value);
            }
        }

        public string ResizeMode
        {
            get
            {

                return this.resizeMode;

            }

            set
            {
                if (this.resizeMode != value)
                {
                    this.resizeMode = value;
                    this.OnPropertyChanged("ResizeMode");
                }

            }
        }
        static FrameworkElement()
        {
            FrameworkPropertyMetadata metadata = new FrameworkPropertyMetadata(DateTime.Now.Year);
            metadata.AffectsArrange = true;
            metadata.AffectsMeasure = true;
            metadata.AffectsParentArrange = true;
            metadata.AffectsParentMeasure = true;
            metadata.AffectsRender = true;
            YearProperty = DependencyProperty.Register("Year", typeof(int), typeof(FrameworkElement), metadata);
        }
    }
    public partial class MainWindow : Window, INotifyPropertyChanged
    {

        private Months months;
        private string white = "#FEFEFE";
        private string transparent = "#00FFFFFF";
        private bool showHolidays = false;
        private FrameworkElement element = new FrameworkElement();
        public Months Months
        {
            get
            {
                return this.months;
            }

            set
            {
                if (this.months != value)
                {
                    this.months = value;
                    this.OnPropertyChanged("Months");
                }

            }
        }


        public FrameworkElement Element
        {
            get
            {
                return this.element;
            }

            set
            {
                if (this.element != value)
                {
                    this.element = value;
                    this.OnPropertyChanged("Element");
                }
            }
        }

        public bool ShowHolidays
        {
            get
            {
                return this.showHolidays;
            }

            set
            {
                if (this.showHolidays != value)
                {
                    this.showHolidays = value;
                    this.OnPropertyChanged("ShowHolidays");
                }

            }
        }


        private void OnPropertyChanged(string propertyName)
        {
            if (this.PropertyChanged != null)
            {
                this.PropertyChanged(this, new PropertyChangedEventArgs(propertyName));
            }
        }

        public event PropertyChangedEventHandler PropertyChanged;

        public MainWindow()
        {
            InitializeComponent();
            element.ResizeMode = "None";
            DataContext = Element;
            this.Months = RunYear(Element.Year);
        }

        private Months RunYear(int year)
        {
            Months months = new Months(year, ShowHolidays);
            grid1.DataContext = months.Items1[0];
            grid2.DataContext = months.Items1[1];
            grid3.DataContext = months.Items1[2];
            grid4.DataContext = months.Items1[3];
            grid5.DataContext = months.Items1[4];
            grid6.DataContext = months.Items1[5];
            grid7.DataContext = months.Items1[6];
            grid8.DataContext = months.Items1[7];
            grid9.DataContext = months.Items1[8];
            grid10.DataContext = months.Items1[9];
            grid11.DataContext = months.Items1[10];
            grid12.DataContext = months.Items1[11];
            return months;
        }

        private void Button_ClickSetYear(object sender, RoutedEventArgs e)
        {
            this.element.Year = int.Parse(yearTxtBox.Text);
            var cols = bigGrid.ColumnDefinitions;
            var rows = bigGrid.RowDefinitions;
            if (cols.Count == 1 && (rows.Count == 1 || rows.Count == 3))
            {
                this.Button_comboClickViewThin(sender, e);
            }
            this.Months = RunYear(Element.Year);
            popLink.IsOpen = false;

            this.Element.ResizeMode = "None";
        }

        private void Button_ClickExit(object sender, RoutedEventArgs e)
        {
            this.Close();
        }

        private void Button_Click_Popup(object sender, RoutedEventArgs e)
        {
            if (popLink.IsOpen == false)
            {
                popLink.IsOpen = true;

            }
            else
            {
                popLink.IsOpen = false;
            }
            //this.Element.ResizeMode = "None";
        }

        private void Window_MouseLeftButtonDown(object sender, MouseButtonEventArgs e)
        {
            this.DragMove();
            this.Element.ResizeMode = "None";
        }

        private void Button_comboClick(object sender, RoutedEventArgs e)
        {
            ComboBoxItem a = (ComboBoxItem)e.OriginalSource;
            Binding binding = new Binding();
            binding.Source = a;
            binding.Path = new PropertyPath("Background");
            binding.Mode = BindingMode.TwoWay;
            WinBack.SetBinding(Path.FillProperty, binding);
            popLink.IsOpen = false;
            this.Element.ResizeMode = "None";
        }



        private void Button_comboClickViewLarge(object sender, RoutedEventArgs e)
        {
            var grids = bigGrid.Children;
            foreach (var item in grids)
            {
                var grid = item as Grid;
                if (grid != null)
                {
                    grid.Opacity = 1;
                }
            }
            var rows = bigGrid.RowDefinitions;
            var cols = bigGrid.ColumnDefinitions;

            if (!(cols.Count == 4 && rows.Count == 3))
            {
                string color = white;
                Binding binding = new Binding();
                binding.Source = color;
                set4.SetBinding(ForegroundProperty, binding);
                //string transparent = this.transparent;
                Binding tBinding = new Binding();
                tBinding.Source = transparent;
                set1.SetBinding(ForegroundProperty, tBinding);
                set2.Foreground = set1.Foreground;
                set3.Foreground = set1.Foreground;
                set5.Foreground = set1.Foreground;
                set6.Foreground = set1.Foreground;
                set7.Foreground = set1.Foreground;
                set8.Foreground = set1.Foreground;
                set9.Foreground = set1.Foreground;
                set10.Foreground = set1.Foreground;
                set11.Foreground = set1.Foreground;
                set12.Foreground = set1.Foreground;

                rows.Clear();
                cols.Clear();
                var col1 = new ColumnDefinition();
                var col2 = new ColumnDefinition();
                var col3 = new ColumnDefinition();
                var col4 = new ColumnDefinition();
                var row1 = new RowDefinition();
                var row2 = new RowDefinition();
                var row3 = new RowDefinition();
                cols.Add(col1);
                cols.Add(col2);
                cols.Add(col3);
                cols.Add(col4);
                rows.Add(row1);
                rows.Add(row2);
                rows.Add(row3);
                Grid.SetRow(grid1, 0); Grid.SetColumn(grid1, 0);
                Grid.SetRow(grid2, 0); Grid.SetColumn(grid2, 1);
                Grid.SetRow(grid3, 0); Grid.SetColumn(grid3, 2);
                Grid.SetRow(grid4, 0); Grid.SetColumn(grid4, 3);
                Grid.SetRow(grid5, 1); Grid.SetColumn(grid5, 0);
                Grid.SetRow(grid6, 1); Grid.SetColumn(grid6, 1);
                Grid.SetRow(grid7, 1); Grid.SetColumn(grid7, 2);
                Grid.SetRow(grid8, 1); Grid.SetColumn(grid8, 3);
                Grid.SetRow(grid9, 2); Grid.SetColumn(grid9, 0);
                Grid.SetRow(grid10, 2); Grid.SetColumn(grid10, 1);
                Grid.SetRow(grid11, 2); Grid.SetColumn(grid11, 2);
                Grid.SetRow(grid12, 2); Grid.SetColumn(grid12, 3);
                this.MinHeight = 150;
                this.MaxHeight = 1000;
                this.MaxWidth = MaxHeight * 4 / 3;
                this.MinWidth = MinHeight * 4 / 3;
                //this.Height = 600;
                this.MaxHeight = 900;
                this.MaxWidth = MaxHeight * 4 / 3;
                this.MinHeight = 450;
                this.MinWidth = MinHeight * 4 / 3;

                string path = "M2394,4273L5874,4273C5954,4273,6019,4339,6019,4419L6019,7274C6019,7354,5954,7420,5874,7420L2394,7420C2314,7420,2248,7354,2248,7274L2248,4419C2248,4339,2314,4273,2394,4273z";
                Binding pathBinding = new Binding();
                pathBinding.Source = path;
                var data = WinBack.Data;
                WinBack.SetBinding(Path.DataProperty, pathBinding);
            }
            set4.Opacity = 0;
            this.Element.ResizeMode = "None";
            popLink.IsOpen = false;
        }

        private void Button_comboClickViewThin(object sender, RoutedEventArgs e)
        {
            var grids = bigGrid.Children;
            foreach (var item in grids)
            {
                var grid = item as Grid;
                if (grid != null)
                {
                    grid.Opacity = 1;
                }
            }


            string color = white;
            Binding binding = new Binding();
            binding.Source = color;
            set2.SetBinding(ForegroundProperty, binding);
            //string transparent = "#00FFFFFF";
            Binding tBinding = new Binding();
            tBinding.Source = transparent;
            set1.SetBinding(ForegroundProperty, tBinding);
            set3.Foreground = set1.Foreground;
            set4.Foreground = set1.Foreground;
            set5.Foreground = set1.Foreground;
            set6.Foreground = set1.Foreground;
            set7.Foreground = set1.Foreground;
            set8.Foreground = set1.Foreground;
            set9.Foreground = set1.Foreground;
            set10.Foreground = set1.Foreground;
            set11.Foreground = set1.Foreground;
            set12.Foreground = set1.Foreground;

            var rows = bigGrid.RowDefinitions;
            var cols = bigGrid.ColumnDefinitions;

            if (cols.Count != 2 && rows.Count != 6)
            {
                rows.Clear();
                cols.Clear();
                var col1 = new ColumnDefinition();
                var col2 = new ColumnDefinition();
                var row1 = new RowDefinition();
                var row2 = new RowDefinition();
                var row3 = new RowDefinition();
                var row4 = new RowDefinition();
                var row5 = new RowDefinition();
                var row6 = new RowDefinition();
                cols.Add(col1);
                cols.Add(col2);
                rows.Add(row1);
                rows.Add(row2);
                rows.Add(row3);
                rows.Add(row4);
                rows.Add(row5);
                rows.Add(row6);
                Grid.SetRow(grid1, 0); Grid.SetColumn(grid1, 0);
                Grid.SetRow(grid2, 0); Grid.SetColumn(grid2, 1);
                Grid.SetRow(grid3, 1); Grid.SetColumn(grid3, 0);
                Grid.SetRow(grid4, 1); Grid.SetColumn(grid4, 1);
                Grid.SetRow(grid5, 2); Grid.SetColumn(grid5, 0);
                Grid.SetRow(grid6, 2); Grid.SetColumn(grid6, 1);
                Grid.SetRow(grid7, 3); Grid.SetColumn(grid7, 0);
                Grid.SetRow(grid8, 3); Grid.SetColumn(grid8, 1);
                Grid.SetRow(grid9, 4); Grid.SetColumn(grid9, 0);
                Grid.SetRow(grid10, 4); Grid.SetColumn(grid10, 1);
                Grid.SetRow(grid11, 5); Grid.SetColumn(grid11, 0);
                Grid.SetRow(grid12, 5); Grid.SetColumn(grid12, 1);
                this.MinHeight = 750;
                this.MaxHeight = 1200;
                this.MaxWidth = this.MaxHeight / 3;
                this.MinWidth = this.MinHeight / 3;

                //this.Height = 750;
                this.MaxHeight = 1800;
                this.MaxWidth = this.MaxHeight / 3;
                this.MinHeight = 750;
                this.MinWidth = this.MinHeight / 3;
                //this.MaxWidth = 350;

                string path = "M151,1L2416,1C2498,1,2566,68,2566,151L2566,6680C2566,6762,2498,6830,2416,6830L151,6830C69,6830,1,6762,1,6680L1,151C1,68,69,1,151,1z";
                Binding pathBinding = new Binding();
                pathBinding.Source = path;
                var data = WinBack.Data;
                WinBack.SetBinding(Path.DataProperty, pathBinding);

            }
            this.Element.ResizeMode = "None";
            popLink.IsOpen = false;
        }

        private void Button_comboClickViewOneMonth(object sender, RoutedEventArgs e)
        {
            int currentYear0 = DateTime.Now.Year;
            var dc0 = grid1.DataContext as Month;
            int year = dc0.Year;
            if (year != currentYear0)
            {
                popLink.IsOpen = false;
                return;
            }
            var rows = bigGrid.RowDefinitions;
            var cols = bigGrid.ColumnDefinitions;

            if (!(cols.Count == 1 && rows.Count == 1))
            {
                rows.Clear();
                cols.Clear();
                var col1 = new ColumnDefinition();
                var row1 = new RowDefinition();
                cols.Add(col1);
                rows.Add(row1);


                int currentMonth = DateTime.Now.Month;
                string name = "set" + currentMonth;
                var grids = bigGrid.Children;
                foreach (var item in grids)
                {
                    var grid = item as Grid;
                    if (grid != null)
                    {
                        grid.Opacity = 0;
                        if (grid.Children.Count != 0)
                        {
                            var children = grid.Children;
                            foreach (var entry in children)
                            {
                                var grid1 = entry as Grid;
                                var entryChildren = grid1.Children;
                                foreach (var element in entryChildren)
                                {
                                    if (element is TextBlock)
                                    {
                                        var set = element as TextBlock;
                                        var setName = set.Name as string;
                                        if (setName != null)
                                        {
                                            if (setName == name)
                                            {
                                                string color = white;
                                                Binding binding = new Binding();
                                                binding.Source = color;
                                                set.SetBinding(ForegroundProperty, binding);
                                                grid.Opacity = 1;
                                                Grid.SetRow(grid, 0); Grid.SetColumn(grid, 0);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }



                this.MaxWidth = 300;
                this.MinWidth = 150;
                this.MinHeight = 150;
                this.MaxHeight = 300;
                this.Height = 300;
                this.MaxHeight = 1000;
                this.MaxWidth = 1000;

                string path = "M63,0L764,0C798,0,827,32,827,72L827,873C827,913,798,945,764,945L63,945C28,945,0,913,0,873L0,72C0,32,28,0,63,0z";
                Binding pathBinding = new Binding();
                pathBinding.Source = path;
                var data = WinBack.Data;
                WinBack.SetBinding(Path.DataProperty, pathBinding);

            }


            set4.Opacity = 0;
            this.Element.ResizeMode = "None";
            popLink.IsOpen = false;
        }

        private void Button_comboClickViewThreeMonths(object sender, RoutedEventArgs e)
        {
            int currentYear0 = DateTime.Now.Year;
            var dc0 = grid1.DataContext as Month;
            int year = dc0.Year;
            if (year != currentYear0)
            {
                popLink.IsOpen = false;
                return;
            }
            var rows = bigGrid.RowDefinitions;
            var cols = bigGrid.ColumnDefinitions;




            if (!(cols.Count == 1 && rows.Count == 3))
            {
                //string transparent = "#00FFFFFF";
                Binding tBinding = new Binding();
                tBinding.Source = transparent;
                set1.SetBinding(ForegroundProperty, tBinding);
                set2.Foreground = set1.Foreground;
                set3.Foreground = set1.Foreground;
                set4.Foreground = set1.Foreground;
                set5.Foreground = set1.Foreground;
                set6.Foreground = set1.Foreground;
                set7.Foreground = set1.Foreground;
                set8.Foreground = set1.Foreground;
                set9.Foreground = set1.Foreground;
                set10.Foreground = set1.Foreground;
                set11.Foreground = set1.Foreground;
                set12.Foreground = set1.Foreground;

                rows.Clear();
                cols.Clear();
                var col1 = new ColumnDefinition();
                var row1 = new RowDefinition();
                var row2 = new RowDefinition();
                var row3 = new RowDefinition();
                cols.Add(col1);
                rows.Add(row1);
                rows.Add(row2);
                rows.Add(row3);

                int currentMonth = DateTime.Now.Month;
                var grids = bigGrid.Children;
                for (int i = 1; i < grids.Count; i++)
                {
                    string name = "set" + (currentMonth - 1);
                    var grid = grids[i] as Grid;
                    if (i >= currentMonth - 1 && i <= currentMonth + 1)
                    {

                        if (grid != null)
                        {
                            grid.Opacity = 1;
                            Grid.SetRow(grid, (i - (currentMonth - 1))); Grid.SetColumn(grid, 0);
                        }
                        if (i == currentMonth - 1)
                        {
                            if (grid.Children.Count != 0)
                            {
                                var children = grid.Children;
                                foreach (var entry in children)
                                {
                                    var grid1 = entry as Grid;
                                    var entryChildren = grid1.Children;
                                    foreach (var element in entryChildren)
                                    {
                                        if (element is TextBlock)
                                        {
                                            var set = element as TextBlock;
                                            var setName = set.Name as string;
                                            if (setName != null)
                                            {
                                                if (setName == name)
                                                {
                                                    string color = white;
                                                    Binding binding = new Binding();
                                                    binding.Source = color;
                                                    set.SetBinding(ForegroundProperty, binding);
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }


                    }
                    else
                    {
                        if (grid != null)
                        {
                            grid.Opacity = 0;
                        }
                    }
                    if (currentMonth == 1)
                    {
                        int currentYear = DateTime.Now.Year;
                        var dc = grid12.DataContext as Month;
                        bool holiday = dc.ShowHolidays;
                        Month dec = new Month(12, currentYear - 1, holiday);
                        grid12.Opacity = 1;
                        grid12.DataContext = dec;
                        string color = "#FEFEFE";
                        Binding binding = new Binding();
                        binding.Source = color;
                        set12.SetBinding(ForegroundProperty, binding);
                        Grid.SetRow(grid12, 0); Grid.SetColumn(grid12, 0);



                    }
                    if (currentMonth == 12)
                    {
                        int currentYear = DateTime.Now.Year;
                        var dc = grid1.DataContext as Month;
                        bool holiday = dc.ShowHolidays;
                        Month jan = new Month(1, currentYear + 1, holiday);
                        grid1.Opacity = 1;
                        grid1.DataContext = jan;
                        Grid.SetRow(grid1, 2); Grid.SetColumn(grid1, 0);
                    }

                }

                this.MaxWidth = 200;
                this.MinWidth = 200;
                this.MinHeight = 600;
                this.MaxHeight = 600;
                this.Height = 600;
                this.MaxHeight = 1200;
                this.MaxWidth = MaxHeight / 3;
                this.MinHeight = 300;
                this.MinWidth = MinHeight / 3;

                string path = "M151,1L2416,1C2498,1,2566,68,2566,151L2566,6680C2566,6762,2498,6830,2416,6830L151,6830C69,6830,1,6762,1,6680L1,151C1,68,69,1,151,1z";
                Binding pathBinding = new Binding();
                pathBinding.Source = path;
                var data = WinBack.Data;
                WinBack.SetBinding(Path.DataProperty, pathBinding);
            }

            this.Element.ResizeMode = "None";
            popLink.IsOpen = false;
        }

        private void Window_SizeChanged(object sender, SizeChangedEventArgs e)
        {
            var cols = bigGrid.ColumnDefinitions;
            var rows = bigGrid.RowDefinitions;

            if (cols.Count == 2 && rows.Count == 6)
            {
                Width = Height / 3;

            }
            else if (cols.Count == 4)
            {
                Width = Height * 4 / 3;

            }

            else if (cols.Count == 1 && rows.Count == 1)
            {
                Width = Height;
            }
            else if (cols.Count == 1 && rows.Count == 3)
            {
                Width = Height / 3;
            }
            FontSize = Width / (cols.Count * 15);
            this.Element.ResizeMode = "None";
        }

        private void Button_ClickHolidays(object sender, RoutedEventArgs e)
        {

            if (!ShowHolidays)
            {
                ShowHolidays = true;
            }
            else
                ShowHolidays = false;
            int currentMonth = DateTime.Now.Month;
            if (currentMonth == 1 || currentMonth == 12)
            {

                var grids = bigGrid.Children;
                for (int i = 1; i < grids.Count; i++)
                {
                    var grid = grids[i] as Grid;
                    var dc = grid.DataContext as Month;
                    int currentYear = dc.Year;
                    int monthNumber = dc.Number;
                    Month current = new Month(monthNumber, currentYear, ShowHolidays);
                    grid.DataContext = current;
                }
            }
            else
                this.Months = RunYear(Element.Year);
            popLink.IsOpen = false;
            this.Element.ResizeMode = "None";
        }

        private void Window_MouseEnter(object sender, MouseEventArgs e)
        {
            this.Element.ResizeMode = "CanResizeWithGrip";
            set1.Opacity = 1;
            set2.Opacity = 1;
            set3.Opacity = 1;
            set4.Opacity = 1;
            set5.Opacity = 1;
            set6.Opacity = 1;
            set7.Opacity = 1;
            set8.Opacity = 1;
            set9.Opacity = 1;
            set10.Opacity = 1;
            set11.Opacity = 1;
            set12.Opacity = 1;


        }

        private void Window_MouseLeave(object sender, MouseEventArgs e)
        {
            set1.Opacity = 0;
            set2.Opacity = 0;
            set3.Opacity = 0;
            set4.Opacity = 0;
            set5.Opacity = 0;
            set6.Opacity = 0;
            set7.Opacity = 0;
            set8.Opacity = 0;
            set9.Opacity = 0;
            set10.Opacity = 0;
            set11.Opacity = 0;
            set12.Opacity = 0;
        }
    }

    public class RenderDay
    {
        public int Key { get; set; }
        public int Value { get; set; }
        public string Color { get; set; }

    }
    public class Month : BaseModel
    {
        private string name;
        private int todayRow;
        private int todayCol;
        private int numberOfDays;
        private DayOfWeek firstDay;
        private int year = DateTime.Now.Year;
        private int[] days;
        private int number;
        private Dictionary<int, RenderDay> dayToPlace = new Dictionary<int, RenderDay>();
        private bool showHolidays;
        public string Name
        {
            get
            {
                return this.name;
            }
            set
            {
                if (this.name != value)
                {
                    this.name = value;
                    this.OnPropertyChanged("Name");
                }
            }
        }


        public Dictionary<int, RenderDay> DayToPlace
        {
            get
            {
                return this.dayToPlace;
            }

            set
            {
                if (this.dayToPlace != value)
                {
                    this.dayToPlace = value;
                    this.OnPropertyChanged("DayToPlace");
                }
            }
        }
        public int Thickness { get; set; }
        public int Today { get; set; }
        public int TodayRow
        {
            get
            {
                return this.todayRow;
            }
            set
            {
                if (this.todayRow != value)
                {
                    this.todayRow = value;
                    this.OnPropertyChanged("TodayRow");
                }
            }
        }
        public int TodayCol
        {
            get
            {
                return this.todayCol;
            }
            set
            {
                if (this.todayCol != value)
                {
                    this.todayCol = value;
                    this.OnPropertyChanged("TodayCol");
                }
            }
        }
        public string Leap { get; set; }
        public int[] Days
        {
            get
            {
                return days;
            }

            set
            {
                days = value;
            }
        }

        public int Number
        {
            get
            {
                return this.number;
            }

            set
            {
                if (this.number != value)
                {
                    this.number = value;
                    this.OnPropertyChanged("Number");
                }

            }
        }

        public bool ShowHolidays
        {
            get
            {
                return this.showHolidays;
            }

            set
            {
                if (this.showHolidays != value)
                {
                    this.showHolidays = value;
                    this.OnPropertyChanged("ShowHolidays");
                }

            }
        }

        public int Year
        {
            get
            {
                return this.year;
            }

            set
            {
                if (this.year != value)
                {
                    this.year = value;
                    this.OnPropertyChanged("Year");
                }

            }
        }

        public Month(int month, int year, bool showHolidays)
        {
            this.number = month;
            Today = DateTime.Now.Day;
            this.firstDay = new DateTime(year, month, 1).DayOfWeek;
            int currentMonth = DateTime.Now.Month;
            int currentYear = DateTime.Now.Year;
            this.Year = year;
            this.showHolidays = showHolidays;
            int start = 0;
            switch (month)
            {
                case 1:
                    this.numberOfDays = 31;
                    this.name = "Január";

                    if (currentMonth == 1)
                    {
                        Thickness = 100;

                    }
                    break;
                case 2:
                    this.name = "Február";
                    if (currentMonth == 2)
                    {
                        Thickness = 100;
                    }
                    if (year % 4 == 0)
                    {
                        this.numberOfDays = 29;
                        this.Leap = "29";
                    }
                    else
                        this.numberOfDays = 28;
                    break;
                case 3:
                    this.name = "Március";
                    if (currentMonth == 3)
                    {
                        Thickness = 100;
                    }
                    this.numberOfDays = 31;
                    break;
                case 4:
                    this.name = "Április";
                    if (currentMonth == 4)
                    {
                        Thickness = 100;
                    }
                    this.numberOfDays = 30;
                    break;
                case 5:
                    this.name = "Május";
                    if (currentMonth == 5)
                    {
                        Thickness = 100;
                    }
                    this.numberOfDays = 31;
                    break;
                case 6:
                    this.name = "Június";
                    if (currentMonth == 6)
                    {
                        Thickness = 100;
                    }
                    this.numberOfDays = 30;
                    break;
                case 7:
                    this.name = "Július";
                    if (currentMonth == 7)
                    {
                        Thickness = 100;
                    }
                    this.numberOfDays = 31;
                    break;
                case 8:
                    this.name = "Augusztus";
                    if (currentMonth == 8)
                    {
                        Thickness = 100;
                    }
                    this.numberOfDays = 31;
                    break;
                case 9:
                    this.name = "Szeptember";
                    if (currentMonth == 9)
                    {
                        Thickness = 100;
                    }
                    this.numberOfDays = 30;
                    break;
                case 10:
                    this.name = "Október";
                    if (currentMonth == 10)
                    {
                        Thickness = 100;
                    }
                    this.numberOfDays = 31;
                    break;
                case 11:
                    this.name = "November";
                    if (currentMonth == 11)
                    {
                        Thickness = 100;
                    }
                    this.numberOfDays = 30;
                    break;
                case 12:
                    this.name = "December";
                    if (currentMonth == 12)
                    {
                        Thickness = 100;
                    }
                    this.numberOfDays = 31;
                    break;
                default:
                    break;
            }
            if (year != currentYear)
            {
                Thickness = 0;
            }
            this.days = new int[numberOfDays];
            Enumerable.Range(1, numberOfDays).ToArray().CopyTo(this.days, 0);
            switch (firstDay)
            {
                case DayOfWeek.Sunday:
                    start = 6;
                    break;
                case DayOfWeek.Monday:
                    start = 0;
                    break;
                case DayOfWeek.Tuesday:
                    start = 1;
                    break;
                case DayOfWeek.Wednesday:
                    start = 2;
                    break;
                case DayOfWeek.Thursday:
                    start = 3;
                    break;
                case DayOfWeek.Friday:
                    start = 4;
                    break;
                case DayOfWeek.Saturday:
                    start = 5;
                    break;
                default:
                    break;
            }
            for (int i = 0; i < this.days.Length; i++)
            {
                bool isHoliday = false;
                int day = this.days[i];
                int place = start + i;

                DayOfWeek currentDayOfWeek = new DateTime(year, month, day).DayOfWeek;
                var holyday = new DateTime(year, month, day).DayOfYear;
                if (currentDayOfWeek == DayOfWeek.Saturday ^ currentDayOfWeek == DayOfWeek.Sunday)
                {
                    isHoliday = true;
                }
                string color = "#FEFEFE";

                if (isHoliday && showHolidays)
                {
                    color = "#2B2A29";
                }
                dayToPlace.Add(day, new RenderDay() { Key = place / 7 + 2, Value = place % 7, Color = color });
            }
            this.todayRow = dayToPlace[Today].Key;
            this.todayCol = dayToPlace[Today].Value;
        }

    }
    public class Months : UIElement
    {

        private Month[] items;

        public Month[] Items1
        {
            get
            {
                return items;
            }

            set
            {
                if (items != value)
                {
                    items = value;
                }

            }
        }
        public Months(int year, bool showHolidays)
        {
            this.items = new Month[12] { new Month(1, year, showHolidays), new Month(2, year, showHolidays), new Month(3, year, showHolidays), new Month(4, year, showHolidays), new Month(5, year, showHolidays), new Month(6, year, showHolidays), new Month(7, year, showHolidays), new Month(8, year, showHolidays), new Month(9, year, showHolidays), new Month(10, year, showHolidays), new Month(11, year, showHolidays), new Month(12, year, showHolidays), };
        }

    }
    public class BaseModel : INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler PropertyChanged;

        protected void OnPropertyChanged(string propertyName)
        {
            if (this.PropertyChanged != null)
            {
                this.PropertyChanged(this, new PropertyChangedEventArgs(propertyName));
            }
        }
    }
}
