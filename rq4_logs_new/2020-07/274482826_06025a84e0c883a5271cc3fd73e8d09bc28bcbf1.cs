using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PixelHunter1995
{
    /// <summary>
    /// Holder for global settings, e.g. if we're in debug mode.
    /// Use sparingly.
    /// Implements the singleton pattern.
    /// </summary>
    class GlobalSettings
    {
        private static GlobalSettings instance;

        private GlobalSettings()
        {
        }

        public static GlobalSettings Instance
        {
            get
            {
                if (instance == null)
                {
                    instance = new GlobalSettings();
                }

                return instance;
            }
        }

        public bool Debug { get; set; } = false;
    }
}