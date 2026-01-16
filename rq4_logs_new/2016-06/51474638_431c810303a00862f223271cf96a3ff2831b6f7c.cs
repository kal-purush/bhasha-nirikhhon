using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System;

namespace ICSheet5e.ResourceModifiers
{
    public class ResourceFileManager
    {
        private string appName = "ICSheet5e";
        private string getResourcePath(string forResource)
        {
            var appData = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
            var combinedPath = Path.Combine(appData, appName, forResource);
            if (File.Exists(combinedPath)) { return combinedPath; }
            else { return Path.Combine("Resources", forResource); }
        }

        public string CreatePathForResource(string resource)
        {
            var appData = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
            Directory.CreateDirectory(Path.Combine(appData, appName)); //only creates if it doesn't exist already
            return Path.Combine(appData, appName, resource);
        }

        public string ArmorListPath
        {
            get { return getResourcePath("BasicArmors.xml"); }
        }

        public string WeaponListPath
        {
            get { return getResourcePath("BasicWeapons.xml"); }
        }

        public string ItemListPath
        {
            get { return getResourcePath("BasicItems.xml"); }
        }

        public string ClassFeaturesPath
        {
            get { return getResourcePath("ClassFeatures.xml"); }
        }

        public string RacialFeaturesPath
        {
            get { return getResourcePath("RacialFeatures.xml"); }
        }

        public string SpelllistPath
        {
            get { return getResourcePath("spell_list.xml"); }
        }

        public string SpellDetailsPath
        {
            get { return getResourcePath("SpellList5e.json"); }
        }
    }
}