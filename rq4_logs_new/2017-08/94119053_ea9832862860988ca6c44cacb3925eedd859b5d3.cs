using System;
using System.Configuration;
using EpicYouTubeDownloader.Properties;
using MediaToolkit.Options;

namespace EpicYouTubeDownloader
{
    public class InitialSettingsService
    {
        #region Private Properties

        private string _destination;
        private string _defaultSampleRate;

        #endregion

        //Settings will be saved under C:\Users\<Username>\AppData\Local\EpicYouTubeDownloader\EpicYouTubeDownloader.vsh_Url_jxgv0p0d2w4oderlomgmu4mlwkbiwhp2\1.0.0.0
        public void setupFoders()
        {
            _destination = Environment.GetFolderPath(Environment.SpecialFolder.MyMusic);
            _defaultSampleRate = AudioSampleRate.Default.ToString();

            if (Settings.Default.Destination != "")
            {
                Settings.Default.Destination = _destination;
                Settings.Default.Save();
            }

            if (Settings.Default.SampleRate != "")
            {
                Settings.Default.SampleRate = _defaultSampleRate;
                Settings.Default.Save();
            }
        }
    }
}