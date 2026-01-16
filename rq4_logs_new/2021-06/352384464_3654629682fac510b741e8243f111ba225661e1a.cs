using System;
using System.Collections.Generic;
using System.Text;
using Prism.Behaviors;
using Xamarin.Forms;

namespace ProjectIP.Helpers
{
    class EmptyPickerValidatorBehavior : BehaviorBase<Picker>
    {
        public static readonly BindableProperty IsValidProperty =
            BindableProperty.Create(nameof(IsValid), typeof(bool), typeof(EmailValidationBehavior), false, BindingMode.OneWayToSource);
        public bool IsValid
        {
            get { return (bool)GetValue(IsValidProperty); }
            set
            {
                SetValue(IsValidProperty, value);
                System.Diagnostics.Debug.WriteLine($"Is True being set to: {value} by the EmptyEntryValidatorBehavior");
            }
        }
        protected override void OnAttachedTo(Picker bindable)
        {
            base.OnAttachedTo(bindable);
            AssociatedObject.SelectedIndexChanged += OnSelectedIndexChanged; //wire method to event
        }

        private void OnSelectedIndexChanged(object sender, EventArgs e)
        {
            var picker = sender as Picker;
            if (picker.SelectedItem == null)
            {
                IsValid = false;
            }
            else
            {
                IsValid = true;
            }
            picker.TextColor = IsValid ? Color.Default : Color.Red;
        }
    }
}