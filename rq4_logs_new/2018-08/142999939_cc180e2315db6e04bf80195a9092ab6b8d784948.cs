using NTC_Consolidator.NTC_View;
using System;
using System.Windows.Forms;

namespace NTC_Consolidator
{
    static class Program
    {
        public static string Role = String.Empty;
        public static string UserName = String.Empty;
        public static string Password = String.Empty;

        public static string ICBSPath = String.Empty;
        public static string AAFUsername = String.Empty;
        public static string AAFPassword = String.Empty;
        public static string AAFDbServer = String.Empty;
        public static string AAFDBSource = String.Empty;

        public static string FAMSUsername = String.Empty;
        public static string FAMSPassword = String.Empty;
        public static string FAMSDbServer = String.Empty;
        public static string FAMSDBSource = String.Empty;

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            //Role = _role;
            //UserName = _username;

            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            // Application.Run(new frmCorrespondingGL());
            Application.Run(new frmConsolidator());
            
        }
    }
}