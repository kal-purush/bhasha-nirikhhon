using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.IO;
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
using IHGVM_VideoManeuver;
using IHGVM_VideoSplitter;
using System.Collections.ObjectModel;
using System.Threading;
using System.Drawing;

namespace IHGVM
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class IndexWindow : Window
    {
        public IndexWindow()
        {
            InitializeComponent();

            // Control Settings Initialization
            txtFileName.IsReadOnly = true;
        }

        OpenFileDialog openFileDialog;
        ProcessFrame processFrame;
        FrameReader frameReader = new FrameReader();
        Locator locator = new Locator();


        #region Constants

        #endregion

        private void btnOpenFile_Click(object sender, RoutedEventArgs e)
        {
            this.Reset();
            openFileDialog = new OpenFileDialog();
            openFileDialog.Filter = INDEX_CONSTANTS.FILE_FILTERS;

            //String path = INDEX_CONSTANTS.STRING_PATH;
            if (openFileDialog.ShowDialog() == true && openFileDialog.FileName != null)
            {
                txtFileName.Text = openFileDialog.FileName;                
            }
        }

        private void btnProcess_Click(object sender, RoutedEventArgs e)
        {
            if (openFileDialog != null)
            {
                processFrame = new ProcessFrame(openFileDialog.FileName, frameReader, locator);                
                this.Start();
            }
        }

        private void btnReset_Click(object sender, RoutedEventArgs e)
        {
            this.Reset();
        }

        private void Reset()
        {
            openFileDialog = null;
            txtFileName.Text = "";
            lblFrameLength.Content = "";
            lblFrameRate.Content = "";
            lblCodec.Content = "";
            ActivityTimeList.Items.Clear(); 
        }

        private Thread frameThread = null;
        public bool Running
        {
            get
            {
                if (frameThread != null)
                {
                    if (frameThread.Join(0) == false)
                        return true;
                    Free();
                }
                return false;
            }
        }

        private void Free()
        {
            frameThread = null;
        }

        public void Start()
        {
            if (frameThread == null)
            {
                ProgresserToggle();

                frameThread = new Thread(() => WorkerThread());
                frameThread.Name = processFrame.Source;
                frameThread.Start();
            }
        }

        public void WorkerThread()
        {
            List<Activity> tempActivities;
            try
            {
                frameReader.Read(processFrame.Source);
                Dispatcher.BeginInvoke(new Action(delegate() 
                {
                    pbFrameStatus.Maximum = processFrame.FrameLength;
                    lblFrameLength.Content = INDEX_CONSTANTS.LABLE_FRAME_LENGTH + (processFrame.FrameLength);
                    lblFrameRate.Content = INDEX_CONSTANTS.LABLE_FRAME_RATE + processFrame.FrameRate;
                    lblCodec.Content = INDEX_CONSTANTS.LABLE_CODEC + processFrame.Codec;
                }));

                locator.silhouetteFrame = null;
                while (true)
                {
                    Bitmap bmp = frameReader.GetNextFrame();
                    locator.ProcessFrame(ref bmp, frameReader.FramePosition);
                    //this.SaveFile(bmp);
                    bmp.Dispose();

                    FrameCountDispatcher();

                    if ((frameReader.FramePosition + 1) >= frameReader.VideoStrech.Length)
                        break;
                }

                tempActivities = processFrame.ActivityFetching();
                //tempActivities = new List<Activity>() { new Activity() { StartTime = 0.0, EndTime = 0.0, FrameNumber = 2 }, new Activity() { StartTime = 0.0, EndTime = 0.0, FrameNumber = 2 }, new Activity() { StartTime = 0.0, EndTime = 0.0, FrameNumber = 2 }, new Activity() { StartTime = 0.0, EndTime = 0.0, FrameNumber = 2 }, new Activity() { StartTime = 0.0, EndTime = 0.0, FrameNumber = 2 }, new Activity() { StartTime = 0.0, EndTime = 0.0, FrameNumber = 2 }, new Activity() { StartTime = 0.0, EndTime = 0.0, FrameNumber = 2 }, new Activity() { StartTime = 0.0, EndTime = 0.0, FrameNumber = 2 }, new Activity() { StartTime = 0.0, EndTime = 0.0, FrameNumber = 2 }, new Activity() { StartTime = 0.0, EndTime = 0.0, FrameNumber = 2 }, new Activity() { StartTime = 0.0, EndTime = 0.0, FrameNumber = 2 }, new Activity() { StartTime = 0.0, EndTime = 0.0, FrameNumber = 2 }, new Activity() { StartTime = 0.0, EndTime = 0.0, FrameNumber = 2 } };
                Dispatcher.BeginInvoke(new Action(delegate() 
                {
                    lblProcessedFrames.Content = "Processed Frames: " + processFrame.FrameLength;
                    pbFrameStatus.Value = 0;
                    ActivityTimeList.Items.Clear();
                    for (int i = 0; i < tempActivities.Count; i++)
                    {
                        ActivityTimeList.Items.Add(tempActivities[i]);
                    }                    
                    ProgresserToggle();
                }));

            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine("exception : " + ex.Message);
            }

            this.Free();
        }

        public void FrameCountDispatcher()
        {
            int forEvery = 100;
            //if (frameReader.FramePosition % forEvery == 0) { 
                Dispatcher.BeginInvoke(new Action(delegate() 
                {
                    lblProcessedFrames.Content = "Processed Frames: " + frameReader.FramePosition;
                    pbFrameStatus.Value = frameReader.FramePosition;
                }));
            //}
        }

        public void ProgresserToggle()
        {
            switch (grdProgresser.Visibility)
            {
                case Visibility.Hidden:
                    grdProgresser.Visibility = Visibility.Visible;
                    break;
                case Visibility.Visible:
                    grdProgresser.Visibility = Visibility.Hidden;
                    break;
            }

        }

        public void Stop()
        {
            if (this.Running)
            {
                frameThread.Abort();
            }
        }
    }
}