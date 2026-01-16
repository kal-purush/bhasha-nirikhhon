using MahApps.Metro.Controls;
using MahApps.Metro.Controls.Dialogs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuickLauncher
{
    public class DialogUtil
    {
        public static void showWarning(MetroWindow win, string message)
        {
            simpleMessageBox("Warning", win, message);
        }

        public static void showError(MetroWindow win, string message)
        {
            simpleMessageBox("Error", win, message);
        }

        public static void showInfo(MetroWindow win, string message)
        {
            simpleMessageBox("Information", win, message);
        }

        public async static void simpleMessageBox(string type, MetroWindow win, string message)
        {
            var mySettings = new MetroDialogSettings()
            {
                AffirmativeButtonText = "OK",
                //NegativeButtonText = "OK",
                //FirstAuxiliaryButtonText = "Cancel",
                ColorScheme = MetroDialogColorScheme.Accented// win.MetroDialogOptions.ColorScheme
            };

            await win.ShowMessageAsync(type, message, MessageDialogStyle.Affirmative, mySettings);
        }

        public async static Task<MessageDialogResult> ShowYesNo(string title, MetroWindow win, string message)
        {
            var mySettings = new MetroDialogSettings()
            {
                AffirmativeButtonText = "Yes",
                NegativeButtonText = "No",
                ColorScheme = MetroDialogColorScheme.Accented
            };

            return await win.ShowMessageAsync(title, message, MessageDialogStyle.AffirmativeAndNegative, mySettings);
        }
    }
}