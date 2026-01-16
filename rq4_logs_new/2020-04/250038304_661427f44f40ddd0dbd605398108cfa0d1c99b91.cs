using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.IO;

namespace ODWai2.Misc.Classes
{
    class Configuration
    {
        private static string _config_path = Path.GetFullPath("../../Misc/");
        private const string _config_name = "configs.xml";

        public static void initialize()
        {
            string full_path = _config_path + _config_name;
            if (!File.Exists(full_path))
            {
                XElement configs = new XElement("annotations", new XElement("configs", ""));
                configs.Save(full_path);
            }
        }

        public static int set_python_path(string path)
        {
            if (!File.Exists(path)) { return -1; }

            string full_path = _config_path + _config_name;
            XElement element = XElement.Load(full_path);
            XElement configs = element.Descendants("configs").First();
            configs.SetElementValue("python-path", path);
            element.Save(full_path);
            return 0;
        }

        public static string get_python_path()
        {
            string full_path = _config_path + _config_name;
            if (!File.Exists(full_path)) { return null; }

            XElement element = XElement.Load(full_path);
            if (!element.Descendants("python-path").Any()) { return null; }

            return element.Descendants("python-path").First().Value;
        }
    }
}