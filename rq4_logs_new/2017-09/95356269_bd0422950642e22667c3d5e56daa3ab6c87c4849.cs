using finview;
using finview.Business.Boot;
using System;
using System.Windows.Forms;

namespace WindowsFormsApp1
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);

            FinviewBusinessInit.Init();
            Application.Run(new FinviewMdi());
        }
    }
}