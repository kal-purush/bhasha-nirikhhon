using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Input;

namespace StudentManagement.ViewModel
{
    public class MainAnouncementViewModel:ViewModelBase
    {
           private bool isAnouncementOpen = true;
        public bool IsAnouncementOpen
        {
            get
            {
                return isAnouncementOpen;
            }

            set
            {
                isAnouncementOpen = value;
                OnPropertyChanged("IsAnouncementOpen");
            }
        }

        private bool isDetailAnouncementOpen = false;



        public bool IsDetailAnouncementOpen
        {
            get
            {
                return isDetailAnouncementOpen;
            }

            set
            {
                isDetailAnouncementOpen = value;
                OnPropertyChanged("IsDetailAnouncementOpen");
            }
        }

    
        public MainAnouncementViewModel()
        {

        }
    }
}