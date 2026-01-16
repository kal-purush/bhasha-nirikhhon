using CAL.Client.Models.Cal;
using CAL.ViewModels;
using System.Windows.Input;

namespace CAL.Controls
{
	[XamlCompilation(XamlCompilationOptions.Compile)]
	public partial class CalendarEvent : ContentView
	{
		public static BindableProperty CalendarEventCommandProperty =
			BindableProperty.Create(nameof(CalendarEventCommand), typeof(ICommand), typeof(CalendarEvent), null);

		public CalendarEvent()
		{
			InitializeComponent();
		}

		public ICommand CalendarEventCommand
		{
			get => (ICommand)GetValue(CalendarEventCommandProperty);
			set => SetValue(CalendarEventCommandProperty, value);
		}

		private void TapGestureRecognizer_Tapped(object sender, System.EventArgs e)
		{
			if (BindingContext is Event eventModel)
				CalendarEventCommand?.Execute(eventModel);
		}
	}
}