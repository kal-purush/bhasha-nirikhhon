using ch.hsr.wpf.gadgeothek.domain;
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
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public List<Gadget> Gadgets { get; set; }
        public MainWindow()
        {
            Gadgets = new List<Gadget>
            {
                new Gadget("android")
            };

            InitializeComponent();

            DataContext = this;
        }

        private void NewGadget_Click(object sender, RoutedEventArgs e)
        {
            NewEditGadget window = new NewEditGadget();
            window.ShowDialog();
        }
    }
}