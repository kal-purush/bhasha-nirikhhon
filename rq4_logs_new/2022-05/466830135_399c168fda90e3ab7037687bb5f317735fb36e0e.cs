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
using System.Windows.Navigation;
using System.Windows.Shapes;
using Organizer.Resources;
using System.Diagnostics;
using System.Collections.ObjectModel;

namespace Organizer
{
    /// <summary>
    /// Interaction logic for StatsPage.xaml
    /// </summary>
    public partial class StatsPage : Page
    {
        public StatsPage()
        {
            InitializeComponent();
            listPrograms();
            listProfiles();
        }

        void listPrograms()
        {
            using (DataClasses1DataContext DB = new DataClasses1DataContext())
            {
                List<Program> progList = new List<Program>();
                foreach (Program p in DB.Program)
                {
                    try
                    {
                        progList.Add(p);
                    }
                    catch { }
                }
                programsList.ItemsSource = progList;
            }
        }
        void listProfiles()
        {
            using (DataClasses1DataContext DB = new DataClasses1DataContext())
            {
                List<Profile> profList = new List<Profile>();
                foreach (Profile p in DB.Profile)
                {
                    try
                    {
                        profList.Add(p);
                    }
                    catch { }
                }
                profilesList.ItemsSource = profList;
            }
        }
        public void programSelectionChange(object sender, SelectionChangedEventArgs e)
        {
            if (programsList.SelectedItem != null)
            {
                using (DataClasses1DataContext DB = new DataClasses1DataContext())
                {
                    var selProg = programsList.SelectedItem as Program;
                    var query = from t1 in DB.Time_program
                                where t1.Id_prog == selProg.Id_prog
                                select t1;
                    List<Time_program> times = new List<Time_program>();
                    foreach (var q in query)
                    {
                        times.Add(q);
                    }

                    statsList.ItemsSource = times;
                }
            }
        }
    }
}