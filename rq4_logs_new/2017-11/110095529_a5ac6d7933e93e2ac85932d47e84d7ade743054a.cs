using System;
using System.Windows.Forms;
using PTClient.Logic.LogicController;
using PTClient.GUI.Map;
using System.Collections.Generic;

namespace PTClient.GUI
{
    static class Program
    {

        private static Controller control = Controller.GetController();
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {

            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            IMap map = new MapControl();
            List<String> TurbineNames = control.GetWindTurbineList();
            foreach(String Name in TurbineNames)
            {
                map.AddTurbineMarker(Name, control.GetTurbineLatitude(Name), control.GetTurbineLongitude(Name));
            }
            Application.Run(new Overview());

        }
    }
}