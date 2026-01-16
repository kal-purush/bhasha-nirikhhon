using System.Windows;

namespace EasySave.Views
{
    public partial class ConfirmDeleteWindow : Window
    {
        public bool IsConfirmed { get; private set; } = false;

        public ConfirmDeleteWindow()
        {
            InitializeComponent();
        }

        private void Yes_Click(object sender, RoutedEventArgs e)
        {
            IsConfirmed = true;
            this.DialogResult = true;
            Close();
        }

        private void No_Click(object sender, RoutedEventArgs e)
        {
            this.DialogResult = false;
            Close();
        }
    }
}