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

namespace QueryDesk
{
    /// <summary>
    /// Interaction logic for ActionsWindow.xaml
    /// </summary>
    public partial class ActionsWindow : Window
    {
        public ActionsWindow()
        {
            InitializeComponent();
        }

        public string SelectedActionType()
        {
            return cbActionType.Text;
        }

        public string GetActionParams()
        {
            return edActionParameters.Text;
        }

        private void ButtonCancel_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = false;
        }

        private void ButtonGo_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = true;
        }

        private void FormActionsWindow_Activated(object sender, EventArgs e)
        {
            cbActionType.SelectedIndex = 0;
        }
    }
}