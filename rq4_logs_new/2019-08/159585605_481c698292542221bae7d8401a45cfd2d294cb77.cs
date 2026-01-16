using IPA.Loader;
using IPA.Old;

namespace SaberTailor.Utilities
{
    public static class Utils
    {
        /// <summary>
        /// Check if a BSIPA plugin is enabled
        /// </summary>
        public static bool IsPluginEnabled(string PluginName)
        {
            if (IsPluginPresent(PluginName))
            {
                PluginLoader.PluginInfo CustomSaberInfo = PluginManager.GetPluginFromId(PluginName);
                if (CustomSaberInfo?.Metadata != null)
                {
                    return PluginManager.IsEnabled(CustomSaberInfo.Metadata);
                }
            }

            return false;
        }

        /// <summary>
        /// Check if a plugin exists
        /// </summary>
        public static bool IsPluginPresent(string PluginName)
        {
            // Check in BSIPA
            if (PluginManager.GetPlugin(PluginName) != null ||
                PluginManager.GetPluginFromId(PluginName) != null)
            {
                return true;
            }

#pragma warning disable CS0618 // IPA is obsolete
            // Check in old IPA
            foreach (IPlugin p in PluginManager.Plugins)
            {
                if (p.Name == PluginName)
                {
                    return true;
                }
            }
#pragma warning restore CS0618 // IPA is obsolete

            return false;
        }
    }
}