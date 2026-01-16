using DLToolkit.Forms.Controls;
using System.Collections;
using System.Windows.Input;
using System.Linq;
using Xamarin.Forms;

namespace OnePiece.App.Controls
{
    public class ExtendedFlowListView : FlowListView
    {
        private bool _isLoadingMore;

        #region Bindable Properties

        public static BindableProperty LoadMoreCommandProperty = BindableProperty.Create(nameof(LoadMoreCommand),
            typeof(ICommand), typeof(ExtendedListView));

        #endregion

        public ExtendedFlowListView()
        {
            RegisterEvents();
        }

        public ExtendedFlowListView(ListViewCachingStrategy cachingStrategy) : base(cachingStrategy)
        {
            RegisterEvents();
        }

        private void RegisterEvents()
        {
            ItemAppearing -= OnItemAppearing;
            ItemAppearing += OnItemAppearing;
        }

        public ICommand LoadMoreCommand
        {
            get { return (ICommand)GetValue(LoadMoreCommandProperty); }
            set { SetValue(LoadMoreCommandProperty, value); }
        }

        private void OnItemAppearing(object sender, ItemVisibilityEventArgs e)
        {
            var items = ItemsSource as IList;
            var itemsOnRow = e.Item as IList;
            var lastItemOnRow = itemsOnRow[itemsOnRow.Count - 1];

            if (_isLoadingMore || items == null || items.Count == 0 || LoadMoreCommand == null || !LoadMoreCommand.CanExecute(lastItemOnRow))
                return;

            // Hit the bottom
            if (e.Item == items[items.Count - 1])
            {
                _isLoadingMore = true;

                LoadMoreCommand.Execute(null);

                _isLoadingMore = false;
            }
        }
    }
}