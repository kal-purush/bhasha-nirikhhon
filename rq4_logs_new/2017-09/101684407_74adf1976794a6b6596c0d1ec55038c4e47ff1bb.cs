using System.Configuration;

namespace ToyTrainProject.Shared
{
   public static class AppConfiguration
    {
        public static string UriBase
        {
            get { return ReadSetting("UriBase"); }
            set { AddUpdateAppSettings("UriBase", value); }
        }

        public static string SubscriptionKey
        {
            get { return ReadSetting("SubscriptionKey"); }
            set { AddUpdateAppSettings("SubscriptionKey", value); }
        }

        private static void AddUpdateAppSettings(string key, string value)
        {
            try
            {
                var configFile = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
                var settings = configFile.AppSettings.Settings;
                if (settings[key] == null)
                {
                    settings.Add(key, value);
                }
                else
                {
                    settings[key].Value = value;
                }
                configFile.Save(ConfigurationSaveMode.Modified);
                ConfigurationManager.RefreshSection(configFile.AppSettings.SectionInformation.Name);
            }
            catch (ConfigurationErrorsException)
            {
                System.Windows.MessageBox.Show("Error writing app settings");
            }
        }

        private static string ReadSetting(string key, string defaultValue = "")
        {
            string returnString = defaultValue;
            try
            {
                var appSettings = ConfigurationManager.AppSettings;

                if (appSettings[key] == null)
                {
                    AddUpdateAppSettings(key, defaultValue);
                    return defaultValue;
                }
                else
                {
                    returnString = appSettings[key];
                }
            }
            catch (ConfigurationErrorsException)
            {
                System.Windows.MessageBox.Show("Error reading app settings");
            }

            return returnString;
        }
    }
}