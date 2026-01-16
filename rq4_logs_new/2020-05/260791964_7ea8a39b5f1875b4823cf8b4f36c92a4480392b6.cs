using CitizenFX.Core;
using CitizenFX.Core.Native;
using CitizenFX.Core.UI;
using System;

namespace MyResourceNameClient
{
    public class Main : BaseScript
    {
        public static string name = "Tutorial";
        public static string version = "1.0";
        public static string author = "Abel Gaming";


        public Main()
        {
            EventHandlers["onClientResourceStart"] += new Action<string>(OnClientResourceStart);
        }

        private void OnClientResourceStart(string resourceName)
        {
            if (API.GetCurrentResourceName() != resourceName) return;
            Screen.ShowNotification("Running " + name + "~n~" + "Current Version: " + version);
        }
    }
}