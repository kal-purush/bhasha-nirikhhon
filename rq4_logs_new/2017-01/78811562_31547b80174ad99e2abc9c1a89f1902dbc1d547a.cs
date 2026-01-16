using NewellClark.Wpf.UserControls.ViewModels;
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

namespace NewellClark.Wpf.UserControls.Views
{
	/// <summary>
	/// Interaction logic for SingleLineRegexInputWidget.xaml
	/// </summary>
	public partial class SingleLineRegexInputWidget : UserControl
	{
		public SingleLineRegexInputWidget()
		{
			InitializeComponent();
			_viewModel = new RegexViewModel();
			DataContext = _viewModel;
		}

		private void optionsSelector_SelectionChanged(object sender, SelectionChangedEventArgs e)
		{
			if (optionsSelector.Items.Count != 0)
			{
				optionsSelector.SelectedIndex = 0;
				e.Handled = true;
			}
		}

		private RegexViewModel _viewModel;
	}
}