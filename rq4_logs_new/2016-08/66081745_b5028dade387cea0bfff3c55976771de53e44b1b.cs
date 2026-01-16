using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using System.Windows.Media;
using GMap.NET.WindowsPresentation;
using System.Diagnostics;

namespace Lws.TrackMyPictures
{
    /// <summary>
    /// Interaction logic for PhotoMarker.xaml
    /// </summary>
    public partial class PhotoMarker
    {
        Popup Popup;
        Label Label;
        GMapMarker Marker;
        MainWindow MainWindow;
        public string path
        { get; set; }

        public PhotoMarker(MainWindow window, GMapMarker marker, string title)
        {
            this.InitializeComponent();
            DataContext = this;
            this.MainWindow = window;
            this.Marker = marker;
            path = title;
            Popup = new Popup();
            Label = new Label();

            this.Unloaded += new RoutedEventHandler(PhotoMarker_Unloaded);
            this.SizeChanged += new SizeChangedEventHandler(PhotoMarker_SizeChanged);
            this.MouseLeftButtonDown += new MouseButtonEventHandler(PhotoMarker_MouseLeftButtonDown);
            this.MouseEnter += new MouseEventHandler(PhotoMarker_MouseEnter);
            this.MouseLeave += new MouseEventHandler(PhotoMarker_MouseLeave);
            this.MouseMove += new MouseEventHandler(PhotoMarker_MouseMove);
            this.MouseLeftButtonDown += new MouseButtonEventHandler(PhotoMarker_MouseLeftButtonDown);
        }

        private void PhotoMarker_Unloaded(object sender, RoutedEventArgs e)
        {
            this.Unloaded -= new RoutedEventHandler(PhotoMarker_Unloaded);
            this.SizeChanged -= new SizeChangedEventHandler(PhotoMarker_SizeChanged);
            this.MouseLeftButtonDown -= new MouseButtonEventHandler(PhotoMarker_MouseLeftButtonDown);
            this.MouseEnter -= new MouseEventHandler(PhotoMarker_MouseEnter);
            this.MouseLeave -= new MouseEventHandler(PhotoMarker_MouseLeave);
            this.MouseMove -= new MouseEventHandler(PhotoMarker_MouseMove);
            this.MouseLeftButtonDown -= new MouseButtonEventHandler(PhotoMarker_MouseLeftButtonDown);

            Marker.Shape = null;
            Popup = null;
            Label = null;
        }

        void PhotoMarker_MouseLeftButtonDown(object sender, MouseButtonEventArgs e)
        {

                MainWindow.MapGrid.Visibility = Visibility.Hidden;
            MainWindow.MainMap.CanDragMap = false;
                MainWindow.MarkerGrid.Visibility = Visibility.Visible;
            
        }


        void PhotoMarker_SizeChanged(object sender, SizeChangedEventArgs e)
        {
            Marker.Offset = new Point(-e.NewSize.Width / 2, -e.NewSize.Height);
        }

        void PhotoMarker_MouseMove(object sender, MouseEventArgs e)
        {
           
            if (e.LeftButton == MouseButtonState.Pressed && IsMouseCaptured)
            {
                Point p = e.GetPosition(MainWindow.MainMap);
                Marker.Position = MainWindow.MainMap.FromLocalToLatLng((int)(p.X), (int)(p.Y));
            }
        }

        void PhotoMarker_MouseLeave(object sender, MouseEventArgs e)
        {
            Marker.ZIndex -= 10000;
            Mouse.OverrideCursor = System.Windows.Input.Cursors.Arrow;
        }

        void PhotoMarker_MouseEnter(object sender, MouseEventArgs e)
        {
            Marker.ZIndex += 10000;
            Mouse.OverrideCursor = System.Windows.Input.Cursors.Hand;
        }
    }
}