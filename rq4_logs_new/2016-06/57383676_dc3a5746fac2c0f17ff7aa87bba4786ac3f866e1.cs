using System;
using System.Drawing;
using System.IO;
using System.Threading;
using System.Windows;
using System.Windows.Media.Imaging;
using NAudio.Wave;
using NAudio.Dsp;
using Emgu.CV;
using Emgu.CV.Structure;

namespace SoftwareprojektTheremin
{

    public class SineWaveProvider32 : WaveProvider32
    {
        int sample;

        public SineWaveProvider32()
        {
            Frequency = 1000;
            Amplitude = 0.25f;
            freq = Frequency;
            amp = Amplitude;
            ActualPositionSineWavePositive = false;
            PreviousPositionSineWavePositive = false;
    }

        public float Frequency;
        public float Amplitude;
        private float freq;
        private float amp;
        private bool ActualPositionSineWavePositive, PreviousPositionSineWavePositive;

        public override int Read(float[] buffer, int offset, int sampleCount)
        {
            int sampleRate = WaveFormat.SampleRate;
            for (int n = 0; n < sampleCount; n++)
            {
                buffer[n + offset] = (float)(amp * Math.Sin((2 * Math.PI * sample * freq) / sampleRate));
                
                PreviousPositionSineWavePositive = ActualPositionSineWavePositive;
                ActualPositionSineWavePositive = buffer[n + offset] >= 0;
                
                sample++;
                
                if (sample >= sampleRate) sample = 0;
                
                if (PreviousPositionSineWavePositive == false && ActualPositionSineWavePositive == true)
                {
                    freq = Frequency;
                    amp = Amplitude;
                }
                //@Note Wenn das nicht funktioniert, NumberOfBuffers und DesiredLatency von Waveout anpassen
            }
            return sampleCount;
        }
    }


    /// <summary>
    /// Interaktionslogik für MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        enum hand : Int32
        {
            LEFT = 0, RIGHT = 1
        }
        enum cord : Int32
        {
            X = 0, Y = 1
        }
        private PXCMSession session;
        private float[,] blobCoordinates = new float[4, 2] { { -1, -1 }, { -1, -1 }, { -1, -1 }, { -1, -1 } };
        private PXCMSenseManager senseManager;
        private PXCMPersonTrackingModule tracker;
        private Thread update;
        private PXCMBlobModule blobModule;
        private PXCMBlobConfiguration blobConfig;
        private PXCMBlobData blobData;
        private PXCMBlobData.IBlob[] blobList = new PXCMBlobData.IBlob[2];
        private int trackingDistance = 1000;
        private int frequency = 35;
        private float volume = 0.5F;
        Mat img, template, result;

            
            
            
        //  private Thread ton;
        private SineWaveProvider32 sineWaveProvider = new SineWaveProvider32();


        public MainWindow()
        {
            InitializeComponent();

            // Configure RealSense session and SenseManager interface
            session = PXCMSession.CreateInstance();
            senseManager = session.CreateSenseManager();
            senseManager.EnableStream(PXCMCapture.StreamType.STREAM_TYPE_COLOR, 640, 480, 30);
            senseManager.EnableBlob();
            senseManager.Init();

            //Create blobModule from SenseManager
            blobModule = senseManager.QueryBlob();
            blobConfig = blobModule.CreateActiveConfiguration();
            blobConfig.SetMaxBlobs(2);
            blobConfig.SetMaxDistance(trackingDistance);
            blobConfig.SetMaxObjectDepth(100);
            blobConfig.SetMinPixelCount(400);
            blobConfig.EnableColorMapping(true);
            blobConfig.ApplyChanges();
            blobData = blobModule.CreateOutput();

            //Audio
            WaveOut waveOut;
            sineWaveProvider.SetWaveFormat(16000, 1);
            sineWaveProvider.Frequency = 1000;
            sineWaveProvider.Amplitude = 0;
            waveOut = new WaveOut();
			waveOut.Init(sineWaveProvider);
            waveOut.Play();

            // Start Update thread
            update = new Thread(new ThreadStart(Update));
            update.Start();
        }



        private void Update()
        {
            // Start AcquireFrame-ReleaseFrame loop
            while (senseManager.AcquireFrame(true) >= pxcmStatus.PXCM_STATUS_NO_ERROR)
            {
                PXCMCapture.Sample sample = senseManager.QuerySample();

                Bitmap colorBitmap;
                PXCMImage.ImageData colorData;
                blobData.Update();

                /*while (blobData.QueryNumberOfBlobs() < 2)
                {
                    trackingDistance += 100;
                    blobConfig.SetMaxDistance(trackingDistance);
                    blobConfig.ApplyChanges();
                    blobData.Update();
                    
                    senseManager.ReleaseFrame();

                   if(trackingDistance > 3000)
                       trackingDistance = 600;
                   }
                   {
                }
                */

                for (int i = 0; i < 2; i++)
                {
                    blobData.QueryBlobByAccessOrder(i, PXCMBlobData.AccessOrderType.ACCESS_ORDER_NEAR_TO_FAR, out blobList[i]);
                }
                if (blobData.QueryNumberOfBlobs() == 2)
                {
                    if (blobCoordinates[3, (int)cord.Y] == -1)
                    {
                        //smoothing
                    }
                    if (blobList[0].QueryExtremityPoint(PXCMBlobData.ExtremityType.EXTREMITY_CENTER).x > blobList[1].QueryExtremityPoint(PXCMBlobData.ExtremityType.EXTREMITY_CENTER).x)
                    {
                        blobCoordinates[(int)hand.LEFT, (int)cord.Y] = blobList[0].QueryExtremityPoint(PXCMBlobData.ExtremityType.EXTREMITY_CENTER).y;
                        blobCoordinates[(int)hand.RIGHT, (int)cord.Y] = blobList[1].QueryExtremityPoint(PXCMBlobData.ExtremityType.EXTREMITY_CENTER).y;
                        blobCoordinates[(int)hand.LEFT, (int)cord.X] = blobList[0].QueryExtremityPoint(PXCMBlobData.ExtremityType.EXTREMITY_CENTER).x;
                        blobCoordinates[(int)hand.RIGHT, (int)cord.X] = blobList[1].QueryExtremityPoint(PXCMBlobData.ExtremityType.EXTREMITY_CENTER).x;
                    }
                    else
                    {
                        blobCoordinates[(int)hand.RIGHT, (int)cord.Y] = blobList[0].QueryExtremityPoint(PXCMBlobData.ExtremityType.EXTREMITY_CENTER).y;
                        blobCoordinates[(int)hand.LEFT, (int)cord.Y] = blobList[1].QueryExtremityPoint(PXCMBlobData.ExtremityType.EXTREMITY_CENTER).y;
                        blobCoordinates[(int)hand.RIGHT, (int)cord.X] = blobList[0].QueryExtremityPoint(PXCMBlobData.ExtremityType.EXTREMITY_CENTER).x;
                        blobCoordinates[(int)hand.LEFT, (int)cord.X] = blobList[1].QueryExtremityPoint(PXCMBlobData.ExtremityType.EXTREMITY_CENTER).x;
                    }
                }

                // Tonausgabe: aktuelle Tonausgabe beenden und neue beginnen
                frequency = (int)(blobCoordinates[(int)hand.LEFT, (int)cord.Y] * 1.8);
                volume = (500 - blobCoordinates[(int)hand.RIGHT, (int)cord.Y]) / 500;

                sineWaveProvider.Frequency = frequency;
                sineWaveProvider.Amplitude = volume;

                // Get color image data
                sample.color.AcquireAccess(PXCMImage.Access.ACCESS_READ, PXCMImage.PixelFormat.PIXEL_FORMAT_RGB32, out colorData);
                colorBitmap = colorData.ToBitmap(0, sample.color.info.width, sample.color.info.height);

                // Update UI
                //colorBitmap.Save("bitmap.bmp");
                //img = CvInvoke.Imread("bitmap.bmp", Emgu.CV.CvEnum.LoadImageType.AnyColor);
                //MatchingMethod();
                Render(colorBitmap);

                // Release frame
                colorBitmap.Dispose();
                sample.color.ReleaseAccess(colorData);
                senseManager.ReleaseFrame();
            }
        }

        private void Render(Bitmap bitmap)
        {
            BitmapImage bitmapImage = ConvertBitmap(bitmap);

            if (bitmapImage != null)
            {
                // Update the WPF Image control
                this.Dispatcher.Invoke(System.Windows.Threading.DispatcherPriority.Normal, new Action(delegate ()
                {
                    imgStream.Source = bitmapImage;
                    labelR.Content = blobCoordinates[(int)hand.RIGHT, (int)cord.Y];
                    labelL.Content = blobCoordinates[(int)hand.LEFT, (int)cord.Y];
                }));
            }
        }

        private BitmapImage ConvertBitmap(Bitmap bitmap)
        {
            BitmapImage bitmapImage = null;
            System.Drawing.Pen fancyPen = new System.Drawing.Pen(System.Drawing.Color.Cyan, 5);

            if (bitmap != null)
            {
                if (blobData.QueryNumberOfBlobs() == 2)
                {
                    for (int i = 0; i < 2; i++)
                    {
                        using (var graphics = Graphics.FromImage(bitmap))
                        {
                            //graphics.DrawRectangle(fancyPen, blobList[i].QueryExtremityPoint(PXCMBlobData.ExtremityType.EXTREMITY_CENTER).x, blobList[i].QueryExtremityPoint(PXCMBlobData.ExtremityType.EXTREMITY_CENTER).y, 5, 5);
                            graphics.DrawRectangle(fancyPen, blobCoordinates[(int)hand.LEFT, (int)cord.X] - 50, blobCoordinates[(int)hand.LEFT, (int)cord.Y], 5, 5);
                            graphics.DrawRectangle(fancyPen, blobCoordinates[(int)hand.RIGHT, (int)cord.X] - 50, blobCoordinates[(int)hand.RIGHT, (int)cord.Y], 5, 5);

                        }
                    }
                }
                bitmap.RotateFlip(RotateFlipType.Rotate180FlipY);
                MemoryStream memoryStream = new MemoryStream();
                bitmap.Save(memoryStream, System.Drawing.Imaging.ImageFormat.Bmp);
                memoryStream.Position = 0;
                bitmapImage = new BitmapImage();
                bitmapImage.BeginInit();
                bitmapImage.StreamSource = memoryStream;
                bitmapImage.CacheOption = BitmapCacheOption.OnLoad;
                bitmapImage.EndInit();
                bitmapImage.Freeze();
            }

            return bitmapImage;
        }

        private void Window_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            ShutDown();
        }

        private void btnExit_Click(object sender, RoutedEventArgs e)
        {
            ShutDown();
            this.Close();
        }

        private void ShutDown()
        {
            // Stop the Update thread
            update.Abort();

            // Dispose RealSense objects
            blobModule.Dispose();
            blobData.Dispose();
            senseManager.Dispose();
            session.Dispose();
        }

        void MatchingMethod()
        {
            /// Source image to display
            Mat img_display = null;
            img.CopyTo(img_display);

            /// Create the result matrix
            int result_cols = img.Cols - template.Cols + 1;
            int result_rows = img.Rows - template.Rows + 1;

            result.Create(result_rows, result_cols, Emgu.CV.CvEnum.DepthType.Cv32F, 1);

            /// Do the Matching and Normalize
            CvInvoke.MatchTemplate(img, template, result, Emgu.CV.CvEnum.TemplateMatchingType.CcoeffNormed);
            CvInvoke.Normalize(result, result, 0, 1, Emgu.CV.CvEnum.NormType.MinMax, Emgu.CV.CvEnum.DepthType.Default, null);

            /// Localizing the best match with minMaxLoc
            double minVal = 0; double maxVal = 0;
            System.Drawing.Point minLoc = new System.Drawing.Point(0,0), maxLoc = new System.Drawing.Point(0, 0), matchLoc=new System.Drawing.Point(0, 0);

            CvInvoke.MinMaxLoc(result, ref minVal, ref maxVal, ref minLoc, ref maxLoc, null);

            /// For SQDIFF and SQDIFF_NORMED, the best matches are lower values. For all the other methods, the higher the better
            /*if (match_method == CV_TM_SQDIFF || match_method == CV_TM_SQDIFF_NORMED)
            { matchLoc = minLoc; }
            else
            { */
            matchLoc = maxLoc; 

            /// Show me what you got
            CvInvoke.Rectangle(img_display, matchLoc, new System.Drawing.Point(matchLoc.X + template.Cols, matchLoc.Y + template.Rows), 
            CvInvoke.Rectangle(result, matchLoc, new System.Drawing.Point(matchLoc.X + template.Cols, matchLoc.Y + template.Rows), CvInvoke.Scalar::all(0), 2, 8, 0);

            //CvInvoke.Imshow(image_window, img_display);
            //CvInvoke.Imshow(result_window, result);

            return;
        }
    }
}