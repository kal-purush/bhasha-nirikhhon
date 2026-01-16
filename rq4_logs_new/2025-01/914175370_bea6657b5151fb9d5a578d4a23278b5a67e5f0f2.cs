namespace PenumbraModForwarder.Common.Models;

public class OldConfigModel
{
    /// <summary>
    /// Represents the old configuration model from the separate project.
    /// </summary>
    public class OldConfigurationModel
    {
        public bool AutoLoad { get; set; }
        public bool AutoDelete { get; set; }
        public bool ExtractAll { get; set; }
        public bool NotificationEnabled { get; set; }
        public bool FileLinkingEnabled { get; set; }
        public bool StartOnBoot { get; set; }
        public string DownloadPath { get; set; } = string.Empty;
        public string TexToolPath { get; set; } = string.Empty;
        public AdvancedConfigurationModel AdvancedOptions { get; set; } = new();
    }

    /// <summary>
    /// Represents advanced configuration options from the old project.
    /// </summary>
    public class OldAdvancedConfigurationModel
    {
        public bool HideWindowOnStartup { get; set; } = true;
        /// <summary>
        /// This timeout is specified in seconds.
        /// </summary>
        public int PenumbraTimeOutInSeconds { get; set; } = 60;
    }
}