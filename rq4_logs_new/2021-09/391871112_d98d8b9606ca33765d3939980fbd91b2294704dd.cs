using System;
using System.Collections.Generic;
using System.Windows.Input;
using Telerik.XamarinForms.Input;
using Xamarin.Forms;

namespace ConasiCRM.Portable.Controls
{
    public partial class DatePickerControl : ContentView
    {
        public event EventHandler DateSelected;

        public static readonly BindableProperty DateProperty = BindableProperty.Create(nameof(Date), typeof(DateTime?), typeof(DatePickerControl), null,BindingMode.TwoWay);
        public DateTime? Date
        {
            get => (DateTime?)GetValue(DateProperty);
            set
            {
                SetValue(DateProperty, value);
                if (Date != value)
                {
                    OnAccept();
                }
            }
        }

        public static readonly BindableProperty PlaceholderProperty = BindableProperty.Create(nameof(Placeholder), typeof(string), typeof(DatePickerControl), null, BindingMode.TwoWay);
        public string Placeholder { get => (string)GetValue(PlaceholderProperty); set => SetValue(PlaceholderProperty, value); }

        public DatePickerControl()
        {
            InitializeComponent();
            radDate.SetBinding(RadDateTimePicker.SelectedDateProperty, new Binding("Date") { Source = this });
            radDate.SetBinding(RadDateTimePicker.PlaceholderProperty, new Binding("Placeholder") { Source = this });
        }

        private void OnAccept()
        {
            DateSelected?.Invoke(this, EventArgs.Empty);
        }
    }
}