using MvvX.Plugins.FileChooser.Wpf;
using System.Linq;
using System.Windows;

namespace MvvX.Plugins.FileChooser.Sample.Wpf
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
        }

        private void btnLogin_Click(object sender, RoutedEventArgs e)
        {
            IFileChooser fileChooser = new WpfFileChooser();

            var result = fileChooser.OpenFiles(false);
            if (result.Result.Value)
            {
                var files = string.Join(System.Environment.NewLine, result.SelectedFiles.Select(f => f.FilePath + " (" + f.FileSize + ")"));
                MessageBox.Show(result.SelectedFiles.Count + " file(s) selected." + System.Environment.NewLine + files);
            }
            else
                MessageBox.Show("Operation canceled");
        }
    }
}