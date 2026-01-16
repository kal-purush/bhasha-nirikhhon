using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Drawing;
using System.Xml.Serialization;
using System.IO;

namespace FacebookApp
{
    public class AppSettings
    {

        public bool RememberUser { get; set; }
        public string LastAccessToken { get; set; }

        public AppSettings()
        {
            RememberUser = false;
            LastAccessToken = null;
        }
        public void saveToFile()
        {
            if (!File.Exists(@"C:\Users\Yarden\Desktop\appSettings.xml"))
            {
                File.Create(@"C:\Users\Yarden\Desktop\appSettings.xml").Close();
            }
            using (Stream stream = new FileStream(@"C:\Users\Yarden\Desktop\appSettings.xml", FileMode.Truncate))
            {
                XmlSerializer serializer = new XmlSerializer(this.GetType());
                serializer.Serialize(stream, this);
            }

        }
        public static AppSettings loadFromFile()
        {
            AppSettings obj = null;
            using (Stream stream = new FileStream(@"C:\Users\Yarden\Desktop\appSettings.xml", FileMode.Open))
            {
                XmlSerializer serializer = new XmlSerializer(typeof(AppSettings));
                try
                {
                    obj = serializer.Deserialize(stream) as AppSettings;
                }
                catch(Exception ex)
                {
                    //Something happened along the way and file is either empty(null) or corrupted
                    obj = new AppSettings();
                }
            }
            return obj;
        }
    }
}