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

namespace Quartz.MARC
{
    /// <summary>
    /// Interaction logic for MarcusHome.xaml
    /// </summary>
    public partial class MarcusHome : Page
    {
        public MarcusHome()
        {
            InitializeComponent();
            GetLoginHistoryLogs();
        }

        //Get the logs and login history of the user 
        public void GetLoginHistoryLogs()
        {
          
        }

        //1. when user clicks to my page, immediately get user login logs 
        //2. 
    }
}