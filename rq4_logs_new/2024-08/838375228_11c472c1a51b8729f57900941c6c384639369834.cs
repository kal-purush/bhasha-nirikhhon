using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Windows;

namespace VideoMerger
{
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
        }

        private void VideoListBox_Drop(object sender, DragEventArgs e)
        {
            if (!e.Data.GetDataPresent(DataFormats.FileDrop)) return;

            var droppedData = e.Data.GetData(DataFormats.FileDrop);

            if (droppedData is not string[] files) return;

            foreach (var file in files)
            {
                if (IsValidVideoFile(file))
                {
                    VideoListBox.Items.Add(file);
                }
            }
        }


        private bool IsValidVideoFile(string file)
        {
            string[] validExtensions = { ".mp4", ".avi", ".mkv", ".webm", ".mov" };
            var extension = Path.GetExtension(file).ToLower();
            return validExtensions.Contains(extension);
        }

        private void CombineButton_Click(object sender, RoutedEventArgs e)
        {
            if (VideoListBox.Items.Count == 0)
            {
                MessageBox.Show("No videos to combine.");
                return;
            }

            List<string> videoFiles = VideoListBox.Items.Cast<string>().ToList();
            CombineVideos(videoFiles);
        }

        private void CombineVideos(List<string> videoFiles)
        {
            // Show SaveFileDialog to select output file location
            Microsoft.Win32.SaveFileDialog saveFileDialog = new Microsoft.Win32.SaveFileDialog();
            saveFileDialog.Filter = "MP4 files (*.mp4)|*.mp4|All files (*.*)|*.*";
            saveFileDialog.Title = "Save Combined Video As";
            saveFileDialog.FileName = "combined_video.mp4"; // Default file name

            if (saveFileDialog.ShowDialog() == true)
            {
                var outputFileName = saveFileDialog.FileName;
                var ffmpegPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "external", "ffmpeg.exe");

                // Create a temporary file list for FFmpeg
                var tempFileList = Path.GetTempFileName();
                using (var writer = new StreamWriter(tempFileList))
                {
                    foreach (var file in videoFiles)
                    {
                        writer.WriteLine($"file '{file}'");
                    }
                }

                // Start FFmpeg process
                var ffmpeg = new Process();
                ffmpeg.StartInfo.FileName = ffmpegPath;
                ffmpeg.StartInfo.Arguments = $"-f concat -safe 0 -i \"{tempFileList}\" -c copy \"{outputFileName}\"";
                ffmpeg.StartInfo.UseShellExecute = false;
                ffmpeg.StartInfo.RedirectStandardOutput = true;
                ffmpeg.StartInfo.RedirectStandardError = true;
                ffmpeg.Start();
                ffmpeg.WaitForExit();

                // Clean up
                File.Delete(tempFileList);

                MessageBox.Show("Videos combined successfully!", "Success", MessageBoxButton.OK,
                    MessageBoxImage.Information);
            }
            else
            {
                MessageBox.Show("Save operation was canceled.", "Canceled", MessageBoxButton.OK,
                    MessageBoxImage.Warning);
            }
        }
    }
}