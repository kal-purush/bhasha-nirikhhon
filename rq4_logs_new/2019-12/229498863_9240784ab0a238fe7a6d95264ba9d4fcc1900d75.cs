using System;
using System.Collections.Generic;
using System.Text;
using Xamarin.Forms;

namespace FormsMvvm2019
{
    public class ListViewBehavior : Behavior<ListView>
    {
        protected override void OnAttachedTo(ListView bindable)
        {
            bindable.ItemSelected += Bindable_ItemSelected;
            base.OnAttachedTo(bindable);
        }

        protected override void OnDetachingFrom(ListView bindable)
        {
            bindable.ItemSelected -= Bindable_ItemSelected;
            base.OnDetachingFrom(bindable);
        }

        private void Bindable_ItemSelected(object sender, SelectedItemChangedEventArgs e)
        {
            var listView = sender as ListView;
            if (e.SelectedItem != null)
            {
                ItemSelectedCommand?.Execute(e.SelectedItem);
                listView.SelectedItem = null;
            }
        }

        public Command ItemSelectedCommand
        {
            get { return (Command)GetValue(ItemSelectedCommandProperty); }
            set { SetValue(ItemSelectedCommandProperty, value); }
        }

        public static readonly BindableProperty ItemSelectedCommandProperty =
            BindableProperty.Create("ItemSelectedCommand", typeof(Command), typeof(ListViewBehavior));
    }
}