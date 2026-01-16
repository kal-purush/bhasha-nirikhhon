using FactorSADEfficiencyOptimizer.Models;
using FactorySADEfficiencyOptimizer.ViewModel;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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

namespace FactorySADEfficiencyOptimizer.UIComponent
{
    /// <summary>
    /// Interaction logic for IndividualTargetWindow.xaml
    /// </summary>
    public partial class IndividualTargetWindow : Window
    {

        public IndividualTargetModel ItemModel { get; set; }


        public IndividualTargetWindow()
        {
            InitializeComponent();
        }
    }
}