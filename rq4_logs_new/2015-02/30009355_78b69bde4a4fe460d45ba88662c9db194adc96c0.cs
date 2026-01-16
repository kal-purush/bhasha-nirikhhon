using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace udesign
{
    public class GVars
    {
        public static void SetRecentAccessedDirectory(string recentDir)
        {
            Properties.Settings.Default.RecentAccessedDir = recentDir;
            Properties.Settings.Default.Save();
        }
    }
}