using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;
using PDGT.Pages;

namespace PDGT.Models
{
    public class SessionViewModel
    {
        public ObservableCollection<SessionView> Sessions { get; set; }
    
        public SessionViewModel()
        {
            Sessions = new ObservableCollection<SessionView>
            {
                    new SessionView
                    {
                        SessionTitle = "Rotator cuff strengthening I",
                        Sets = "3",
                        Reps = "8",
                        WeightInKg = "3-5"
                    },

                    new SessionView
                    {
                        SessionTitle = "Rotator cuff strengthening II",
                        Sets = "5",
                        Reps = "10",
                        WeightInKg = "3-5"
                    },

                    new SessionView
                    {
                        SessionTitle = "Rotator cuff strengthening III",
                        Sets = "5",
                        Reps = "10",
                        WeightInKg = "3-5"
                    }
                    
            };
        }
    }
}