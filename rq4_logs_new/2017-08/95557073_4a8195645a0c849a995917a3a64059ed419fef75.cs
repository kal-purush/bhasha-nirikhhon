using System.IO;
using System.Threading.Tasks;

namespace ClashOfTanks.GUI.Utility
{
    sealed class SettingsFileProcessor
    {
        private string SettingsFilePath { get; } = "Settings.ini";

        public async Task<string> ReadSettings(string settingsName)
        {
            using (StreamReader reader = new StreamReader(File.Open(SettingsFilePath, FileMode.Open, FileAccess.Read)))
            {
                string line;

                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (line.Contains("=") && line.Remove(line.IndexOf('=')).ToLower().Trim() == settingsName.ToLower())
                    {
                        return line.Substring(line.IndexOf('=') + 1).Trim();
                    }
                }

                return null;
            }
        }

        public async void WriteSettings(string settingsName, string settingsValue)
        {

        }
    }
}