using NewellClark.Wpf.UserControls.TypeConverters;
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

namespace TestApp.ManualTestWidgets
{
	/// <summary>
	/// Interaction logic for IsValidPropertyBindingTest.xaml
	/// </summary>
	public partial class IsValidPropertyBindingTest : UserControl
	{
		public IsValidPropertyBindingTest()
		{
			InitializeComponent();

			var binding = new Binding(nameof(regexInput.IsValid));
			binding.Source = regexInput;
			binding.Converter = new BooleanToToggleConverter<Visibility>(
				Visibility.Hidden, Visibility.Visible);
			binding.Mode = BindingMode.OneWay;
			invalidMessageViewer.SetBinding(
				TextBlock.VisibilityProperty, binding);
		}
	}
}