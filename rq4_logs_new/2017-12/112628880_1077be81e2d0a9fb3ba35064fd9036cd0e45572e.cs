using ch.hsr.wpf.gadgeothek.domain;
using ch.hsr.wpf.gadgeothek.ui.ViewModel;
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

namespace ch.hsr.wpf.gadgeothek.ui
{
    /// <summary>
    /// Interaction logic for EditGadget.xaml
    /// </summary>
    public partial class EditGadget : Window
    {
        public EditGadget(Gadget gadget)
        {
            InitializeComponent();
            EditGadgetViewModel editGadgetViewModel = new EditGadgetViewModel(gadget);
            DataContext = editGadgetViewModel;

            editGadgetViewModel.CanClose += (o, a) => this.Close();
        }
    }
}