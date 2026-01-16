using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;

using MahApps.Metro.Controls;
using MahApps.Metro;

namespace PPGit.GUI.Deadlines
{
    /// <summary>
    /// Interaction logic for deadlineInfo.xaml
    /// </summary>
    public partial class deadlineInfo : MetroWindow
    {
        PurpleProse.Lib.deadline thisDeadline;
        public deadlineInfo(PurpleProse.Lib.deadline myDeadline)
        {
            InitializeComponent();
            thisDeadline = myDeadline;
        }

        private void btnRightWindowChangeTheme_Click(object sender, RoutedEventArgs e)
        {
            AppThemeChanger wnd = new AppThemeChanger();

            wnd.Show();
        }

        private void deadlineInfo1_Loaded(object sender, RoutedEventArgs e)
        {
            DateTime theDate = thisDeadline.getDate;
            string month = Lib.time.Name;
            string day = theDate.Day.ToString();
            this.Title = day + ", " + month;
            //Setting the Word-Count
            int words = thisDeadline.theWordCount;
            if (words == 0)
            {
                wordsLeftLBL.IsEnabled = false;
                wrdsLftTXT.IsEnabled = false;
            }
            else {
                wrdsLftTXT.Text = words.ToString();
            }
            //Setting the notes
            string notes = thisDeadline.getSetNotes;
            if (notes == null)
            {
                notesLBL.IsEnabled = false;
                notesTXT.IsEnabled = false;
            }
            else {
                notesTXT.Text = notes;
            }
        }

        private void updateBTN_Click(object sender, RoutedEventArgs e)
        {
            bool updated = false;
            if (wrdsLftTXT.IsEnabled)
            {
                try
                {
                    thisDeadline.theWordCount = Convert.ToInt32(wrdsLftTXT.Text);
                    updated = true;
                }
                catch (FormatException)
                {
                    MessageBox.Show("Unable to update word count", "ERROR", MessageBoxButton.OK, MessageBoxImage.Warning);
                }
            }

            if (notesTXT.IsEnabled)
            {
                thisDeadline.getSetNotes = notesTXT.Text;
                updated = true;
            }

            if (updated == true)
            {
                MessageBoxResult newResult = MessageBox.Show("Deadline Updated.\nWould you like to go back to the calendar?", "Update", MessageBoxButton.YesNo, MessageBoxImage.Question);
                if (newResult == MessageBoxResult.Yes) this.Close();
            }
        }

        private void clsBTN_Click(object sender, RoutedEventArgs e)
        {
            this.Close();
        }
    }
}