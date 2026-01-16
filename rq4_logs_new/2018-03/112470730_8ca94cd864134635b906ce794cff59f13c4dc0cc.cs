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
using Application;
using WpfApp1.ViewModel;

namespace WpfApp1.Views
{
	/// <summary>
	/// Interaction logic for SelectPermissionWindow.xaml
	/// </summary>
	public partial class SelectPermissionWindow : Window
	{
		SupportMenuViewModel _viewModel;

		public SelectPermissionWindow(SupportMenuViewModel viewModel)
		{
			_viewModel = viewModel;

			InitializeComponent();
			DataContext = viewModel;

			RetrieveAllPermissions();
		}

		public SelectPermissionWindow()
		{
			_viewModel = new SupportMenuViewModel();

			InitializeComponent();
			DataContext = _viewModel;

			RetrieveAllPermissions();
		}

		private void RetrieveAllPermissions()
		{
			string tableContent = PermissionAPI.RetrieveAllPermissions();
			string[] table = tableContent.Split(new string[] { "|n|" }, StringSplitOptions.None);

			for(int i = 1; i < table.Length; i++)
			{
				string[] columns = table[i].Split(new string[] { "|t|" }, StringSplitOptions.None);
				
				string permissionID = columns[0];
				string legalText = columns[1];

				_viewModel.AddPermissionToList(Convert.ToInt32(permissionID), legalText);
			}
		}

		private void btnSelectPermission_Click(object sender, RoutedEventArgs e)
		{
			this.Close();
		}
	}
}