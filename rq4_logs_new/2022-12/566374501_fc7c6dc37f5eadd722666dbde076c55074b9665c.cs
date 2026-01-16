using Standard.Tool.Platform.CommonPage;
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

namespace Standard.Tool.Platform.UserControls
{
    /// <summary>
    /// UserControlOfflineLogin.xaml 的交互逻辑
    /// </summary>
    public partial class UserControlOfflineLogin : UserControl
    {
        public UserControlOfflineLogin()
        {
            InitializeComponent();
            this.DataContext = new LoginPageViewMode();
        }

        private void Image_MouseUp(object sender, MouseButtonEventArgs e)
        {
            Application.Current.Shutdown();
        }
    }
}