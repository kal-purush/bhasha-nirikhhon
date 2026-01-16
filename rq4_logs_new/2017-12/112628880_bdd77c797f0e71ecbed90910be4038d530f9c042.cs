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

namespace ch.hsr.wpf.gadgeothek.ui
{
    /// <summary>
    /// Interaction logic for GadgetPanel.xaml
    /// </summary>
    public partial class GadgetPanel : UserControl
    {
        public GadgetPanel()
        {
            InitializeComponent();
        }
        private void NewGadget_Click(object sender, RoutedEventArgs e)
        {
            NewEditGadget window = new NewEditGadget();
            window.ShowDialog();
        }
    }

}