using AForge.Video;
using AForge.Video.DirectShow;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;

namespace ToyTrainProject
{
    /// <summary>
    /// Interaction logic for Window1.xaml
    /// </summary>
    public partial class Window1 : Window
    {
        private BitmapImage latestFrame;
        VideoCaptureDevice LocalWebCam;
        public FilterInfoCollection LocalWebCamsCollection;
        Action<BitmapImage> captureImage;

        public Window1()
        {
            InitializeComponent();
            this.DataContext = this;

            Loaded += CameraWindow_Loaded;
            Unloaded += CameraWindow_Unloaded;
        }

        void Cam_NewFrame(object sender, NewFrameEventArgs eventArgs)
        {
            //try
            //{
            //    System.Drawing.Image img = (Bitmap)eventArgs.Frame.Clone();
            //    MemoryStream ms = new MemoryStream();
            //    img.Save(ms, System.Drawing.Imaging.ImageFormat.Bmp);
            //    ms.Seek(0, SeekOrigin.Begin);
            //    BitmapImage bi = new BitmapImage();
            //    bi.BeginInit();
            //    bi.StreamSource = ms;
            //    bi.EndInit();
            //    bi.Freeze();
            //    this.latestFrame = bi;
            //    Dispatcher.BeginInvoke(new ThreadStart(delegate
            //    {
            //        videoWindow.Source = bi;
            //    }));
            //}
            //catch (Exception ex)
            //{

            //}

            System.Drawing.Image img = (Bitmap)eventArgs.Frame.Clone();

           
        }

        private void CameraWindow_Loaded(object sender, RoutedEventArgs e)
        {
            LocalWebCamsCollection = new FilterInfoCollection(FilterCategory.VideoInputDevice);
            LocalWebCam = new VideoCaptureDevice(LocalWebCamsCollection[0].MonikerString);
            LocalWebCam.VideoResolution = LocalWebCam.VideoCapabilities[0];
            LocalWebCam.NewFrame += new NewFrameEventHandler(Cam_NewFrame);

            LocalWebCam.Start();
        }

        private void CameraWindow_Unloaded(object sender, RoutedEventArgs e)
        {
            LocalWebCam.NewFrame -= new NewFrameEventHandler(Cam_NewFrame);
            LocalWebCam.Stop();
            LocalWebCam = null;
        }

        private void TakePicButton_Click(object sender, RoutedEventArgs e)
        {
            if (captureImage != null)
            {
                captureImage(latestFrame);
            }
        }

    }
}