using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;
using InvoiceTracker.Forms;

namespace InvoiceTracker
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            //string cs = ConnfigurationStrings.AccessDb;
            using (var db = new InvoiceTracker.Database.InvoiceTrackerDB())
            {
                Application.EnableVisualStyles();
                Application.SetCompatibleTextRenderingDefault(false);
                Main m = new Main
                            {
                                Text = MainHeading(),
                                Model = db
                            };
                Application.Run(m);
            }            
        }

        static string MainHeading()
        {
            return "Water Resources Division: Invoice Tracker Beta 0.0.1";
        }
        
    }
}