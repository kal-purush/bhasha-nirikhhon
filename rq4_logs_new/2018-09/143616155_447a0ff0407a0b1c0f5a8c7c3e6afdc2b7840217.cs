using System.Windows;

namespace Room17DE.MeetingDecline.Forms
{
    /// <summary>
    /// Interaction logic for RulesForm2.xaml
    /// </summary>
    public partial class RulesForm2 : Window
    {
        public RulesForm2()
        {
            InitializeComponent();

            // load and bind data
            mainGrid.ItemsSource = new Util.DeclineRuleController().LoadData();
        }

        private void MessageButton_Click(object sender, RoutedEventArgs e)
        {
            // TODO: open message here
        }

        /// <summary>
        /// Event handler to close the dialog when pressong OK button
        /// </summary>
        private void OKButton_Click(object sender, RoutedEventArgs e)
        {
            this.DialogResult = true;
            this.Close();
        }
    }
}