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

namespace Portfolio.Pages
{
    /// <summary>
    /// Logique d'interaction pour UserControl1.xaml
    /// </summary>
    public partial class CVPageUC : UserControl
    {
        public CVPageUC()
        {
            InitializeComponent();
            InitializeLineLengthInCV();
            InitializeLineLengthInEducation();
        }

        public void InitializeLineLengthInCV()
        {
            Line1.Y2 = ExSolutecCanvas.ActualHeight;
            Line2.Y2 = ExInfineonCanvas.ActualHeight;
            Line3.Y2 = ExCarsoCanvas.ActualHeight;
        }

        public void InitializeLineLengthInEducation()
        {
            Line4.Y2 = FoCPECanvas.ActualHeight;
            Line5.Y2 = FoPrepCPECanvas.ActualHeight;
        }
    }
}