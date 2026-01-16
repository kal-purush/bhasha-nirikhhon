using AForge.Video.DirectShow;
using GalaSoft.MvvmLight.Command;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using ToyTrainProject.Models;

namespace ToyTrainProject.Controls
{
    public partial class Webcam : UserControl
    {
        private static List<DeviceInfo> availableCameraSources;
        private static RelayCommand takeinternalPictureCommand;
        private static Webcam host;
        private static VideoCaptureDevice videoDevice;

        public Webcam()
        {
            this.InitializeComponent();
        }

        private static void OnTakePictureCommandChanged(DependencyObject dependencyObject, DependencyPropertyChangedEventArgs e)
        {
            host = (Webcam)dependencyObject;
        }

        private static object OnCameraCoherceValueChanged(DependencyObject dependencyObject, object basevalue)
        {
            host = (Webcam)dependencyObject;
            if (string.IsNullOrEmpty(basevalue.ToString()))
            {
                return null;
            }

            if (availableCameraSources.FirstOrDefault(c => c.UsbString.Equals(basevalue.ToString())) == null)
            {
                if (availableCameraSources != null)
                {
                    var firstOrDefault = availableCameraSources.FirstOrDefault();
                    if (firstOrDefault != null)
                    {
                        return firstOrDefault.UsbString;
                    }
                }
            }

            return basevalue;
        }


        private static void OnCameraValueChanged(DependencyObject dependencyObject, DependencyPropertyChangedEventArgs e)
        {
            host = (Webcam)dependencyObject;
            if (e.NewValue == null || string.IsNullOrEmpty(e.NewValue.ToString()))
            {

                StopVideoDevice();
                return;
            }


            if (e.NewValue != e.OldValue)
            {
                if (videoDevice != null)
                {
                    StopVideoDevice();
                    InitVideoDevice(e.NewValue.ToString(), (Webcam)dependencyObject);
                }
                else
                {
                    InitVideoDevice(e.NewValue.ToString(), (Webcam)dependencyObject);
                }
            }
        }

        private static RelayCommand TakeInternalPictureCommand
        {
            get
            {
                return takeinternalPictureCommand ?? (takeinternalPictureCommand = new RelayCommand(TakePicture));
            }
        }

        private static void TakePicture()
        {
            try
            {
                const int PanelWidth = 640;
                const int PanelHeight = 480;
                string fileName;

                System.Drawing.Point pnlPoint =
                    host.VideoPlayer.PointToScreen(
                        new System.Drawing.Point(host.VideoPlayer.ClientRectangle.X, host.VideoPlayer.ClientRectangle.Y)); // get the position of the VideoPlayer
                using (var bmp = new Bitmap(PanelWidth, PanelHeight))
                {
                    using (var g = Graphics.FromImage(bmp))
                    {
                        // generate the image
                        g.CopyFromScreen(
                            pnlPoint, System.Drawing.Point.Empty, new System.Drawing.Size(PanelWidth, PanelHeight));
                    }

                    // Get MyPictures folder path
                    fileName = Environment.GetFolderPath(Environment.SpecialFolder.MyPictures) + "\\"
                               + DateTime.Now.ToString().Replace(":", string.Empty).Replace("/", string.Empty) + ".jpg";

                    // save the file
                    bmp.Save(fileName, System.Drawing.Imaging.ImageFormat.Jpeg);
                }

                MessageBox.Show("Picture saved in Pictures library with filename=" + fileName, "Success");
            }
            catch (Exception exception)
            {

                MessageBox.Show(exception.Message);
            }
        }



        private static void InitVideoDevice(string cameraString, Webcam host)
        {

            if (string.IsNullOrEmpty(cameraString))
            {
                StopVideoDevice();
                return;
            }

            videoDevice = new VideoCaptureDevice(cameraString) { DesiredFrameSize = new System.Drawing.Size(640, 480) };
            host.VideoPlayer.VideoSource = videoDevice;
            host.VideoPlayer.Start();

        }


        private static void StopVideoDevice()
        {
            if (videoDevice == null)
            {
                return;
            }

            videoDevice.SignalToStop();
            videoDevice.WaitForStop();
            videoDevice.Stop();
            videoDevice = null;
        }


        private static IList<DeviceInfo> GetCameraSources()
        {
            availableCameraSources = new List<DeviceInfo>();
            var videoDevices = new FilterInfoCollection(FilterCategory.VideoInputDevice);
            foreach (FilterInfo filterInfo in videoDevices)
            {
                availableCameraSources.Add(
                    new DeviceInfo { DisplayName = filterInfo.Name, UsbString = filterInfo.MonikerString });
            }
            return availableCameraSources;
        }


        public static readonly DependencyProperty AvailableCameraSourcesProperty =
            DependencyProperty.Register(
                "AvailableCameraSources",
                typeof(IList<DeviceInfo>),
                typeof(Webcam),
                new PropertyMetadata(GetCameraSources())); //set the default value 


        public static readonly DependencyProperty CameraIdProperty =
            DependencyProperty.Register("CameraId", typeof(string), typeof(Webcam), new PropertyMetadata(string.Empty, OnCameraValueChanged, OnCameraCoherceValueChanged));


        public static readonly DependencyProperty TakePictureCommandProperty =
            DependencyProperty.Register(
                "TakePictureCommand",
                typeof(ICommand),
                typeof(Webcam),
                new PropertyMetadata(TakeInternalPictureCommand));

        public ICommand TakePictureCommand
        {
            get => (ICommand)this.GetValue(TakePictureCommandProperty);

            set => this.SetValue(TakePictureCommandProperty, value);
        }


        public IList<FilterInfo> AvailableCameraSources
        {
            get => (IList<FilterInfo>)this.GetValue(AvailableCameraSourcesProperty);
            set => SetValue(AvailableCameraSourcesProperty, value);
        }


        public string CameraId
        {
            get => (string)this.GetValue(CameraIdProperty);
            set => SetValue(CameraIdProperty, value);
        }

    }
}