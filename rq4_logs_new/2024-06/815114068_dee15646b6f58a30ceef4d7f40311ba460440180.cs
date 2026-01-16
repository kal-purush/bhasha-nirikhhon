using System;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;
using Microsoft.Kinect;

namespace KinectCapture
{
    class Program
    {
        static KinectSensor sensor;
        static MultiSourceFrameReader reader;
        static string colorImagePath = "C:\\KinectData\\color.jpg";
        static string depthImagePath = "C:\\KinectData\\depth.jpg";

        static void Main(string[] args)
        {
            // Ensure the directory exists
            if (!Directory.Exists("C:\\KinectData"))
            {
                Directory.CreateDirectory("C:\\KinectData");
            }

            // Initialize Kinect sensor
            sensor = KinectSensor.GetDefault();
            if (sensor == null)
            {
                Console.WriteLine("No Kinect sensor detected.");
                return;
            }

            // Open the sensor
            sensor.Open();
            reader = sensor.OpenMultiSourceFrameReader(FrameSourceTypes.Color | FrameSourceTypes.Depth);
            reader.MultiSourceFrameArrived += Reader_MultiSourceFrameArrived;

            // Keep the console open
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();

            // Close the sensor
            sensor.Close();
        }

        private static void Reader_MultiSourceFrameArrived(object sender, MultiSourceFrameArrivedEventArgs e)
        {
            var frameReference = e.FrameReference.AcquireFrame();
            if (frameReference == null) return;

            // Handle color frame
            using (var colorFrame = frameReference.ColorFrameReference.AcquireFrame())
            {
                if (colorFrame != null)
                {
                    FrameDescription colorFrameDescription = colorFrame.FrameDescription;
                    byte[] colorPixels = new byte[colorFrameDescription.Width * colorFrameDescription.Height * 4];
                    colorFrame.CopyConvertedFrameDataToArray(colorPixels, ColorImageFormat.Bgra);

                    Bitmap colorBitmap = new Bitmap(colorFrameDescription.Width, colorFrameDescription.Height, PixelFormat.Format32bppArgb);
                    BitmapData colorBitmapData = colorBitmap.LockBits(
                        new Rectangle(0, 0, colorBitmap.Width, colorBitmap.Height),
                        ImageLockMode.WriteOnly,
                        colorBitmap.PixelFormat);

                    IntPtr colorPtr = colorBitmapData.Scan0;
                    System.Runtime.InteropServices.Marshal.Copy(colorPixels, 0, colorPtr, colorPixels.Length);
                    colorBitmap.UnlockBits(colorBitmapData);

                    colorBitmap.Save(colorImagePath, ImageFormat.Jpeg);
                }
            }

            // Handle depth frame
            using (var depthFrame = frameReference.DepthFrameReference.AcquireFrame())
            {
                if (depthFrame != null)
                {
                    FrameDescription depthFrameDescription = depthFrame.FrameDescription;
                    ushort[] depthPixels = new ushort[depthFrameDescription.Width * depthFrameDescription.Height];
                    depthFrame.CopyFrameDataToArray(depthPixels);

                    Bitmap depthBitmap = new Bitmap(depthFrameDescription.Width, depthFrameDescription.Height, PixelFormat.Format16bppGrayScale);
                    BitmapData depthBitmapData = depthBitmap.LockBits(
                        new Rectangle(0, 0, depthBitmap.Width, depthBitmap.Height),
                        ImageLockMode.WriteOnly,
                        depthBitmap.PixelFormat);

                    IntPtr depthPtr = depthBitmapData.Scan0;
                    System.Runtime.InteropServices.Marshal.Copy(depthPixels, 0, depthPtr, depthPixels.Length);
                    depthBitmap.UnlockBits(depthBitmapData);

                    depthBitmap.Save(depthImagePath, ImageFormat.Jpeg);
                }
            }
        }
    }
}