using System.ComponentModel;
using System.Windows.Controls;
using Dartware.Radiocamp.Clients.Windows.Core;
using Dartware.Radiocamp.Clients.Windows.Core.MVVM;

namespace Dartware.Radiocamp.Clients.Windows.Views
{
	
	public abstract class View : UserControl
	{
	}

	public class View<ViewModelType> : View where ViewModelType : ViewModel
	{

		public ViewModelType ViewModel { get; set; }

		public View()
		{
			if (!DesignerProperties.GetIsInDesignMode(this))
			{
				DataContext = ViewModel = Dependencies.Get<ViewModelType>();
			}
		}

	}

	public class DynamicView<ViewModelType> : View where ViewModelType : DynamicViewModel
	{

		public ViewModelType ViewModel { get; set; }

		public DynamicView()
		{
			DataContext = ViewModel = Dependencies.Get<ViewModelType>();
		}

	}

}