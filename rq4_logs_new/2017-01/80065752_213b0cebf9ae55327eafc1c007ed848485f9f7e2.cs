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
using GitBitterLib;

namespace GitBitterEdit
{
    /// <summary>
    /// Interaction logic for RepoSelect.xaml
    /// </summary>
    public partial class RepoSelect : Window
    {
        public RepositoryDescription SelectRepository
        {
            get
            {
                return (RepositoryDescription)lstRepositories.SelectedItem;
            }
        }

        public RepoSelect()
        {
            InitializeComponent();

            var lister = new BitbucketLister();
            lstRepositories.ItemsSource = lister.GetRepositories("GDK");
            lstRepositories.DisplayMemberPath = "Name";
        }

        private void btnCancel_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = false;
        }

        private void btnOk_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = true;
        }

        private void lstRepositories_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            DialogResult = true;
        }
    }
}