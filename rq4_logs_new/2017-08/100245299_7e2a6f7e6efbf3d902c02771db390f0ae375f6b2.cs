namespace Zebble
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;

    partial class Grid<TSource, TCellTemplate>
    {
        int VisibleItems = 0;
        float GridHeight = 0;
        bool IsLazyLoadingMore, lazyLoad;
        ScrollView ParentScroller;

        public bool LazyLoad
        {
            get => lazyLoad;
            set { lazyLoad = value; SetPseudoCssState("lazy-loaded", value).RunInParallel(); }
        }

        async Task OnShown()
        {
            if (LazyLoad)
            {
                ParentScroller = FindParent<ScrollView>();
                ParentScroller?.UserScrolledVertically.HandleOn(Device.ThreadPool, OnUserScrolledVertically);

                await LazyLoadInitialItems();
            }
        }

        async Task OnUserScrolledVertically()
        {
            if (IsLazyLoadingMore) return;
            IsLazyLoadingMore = true;

            var staticallyVisible = ParentScroller.ActualHeight - ActualY;

            var shouldShowUpto = ParentScroller.ScrollY + staticallyVisible + 10 /* Margin to ensure something is there */;

            while (shouldShowUpto >= GridHeight)
            {
                if (!await LazyLoadMore()) break;

                if (Device.Platform == DevicePlatform.IOS) await Task.Delay(Animation.OneFrame);
            }

            IsLazyLoadingMore = false;
        }

        protected override float CalculateContentAutoHeight()
        {
            if (!LazyLoad) return base.CalculateContentAutoHeight();

            var lastItem = ItemViews.LastOrDefault();
            if (lastItem == null) return 0;

            lastItem.ApplyCssToBranch().Wait();

            if (lastItem.Height.AutoOption.HasValue || lastItem.Height.PercentageValue.HasValue)
                Device.Log.Error("Items in a lazy loaded grid must have an explicit height value.");

            return Padding.Vertical() +
                (float)Math.Ceiling((double)dataSource.Count / Columns) * lastItem.CalculateTotalHeight();
        }

        Task LazyLoadInitialItems() => UIWorkBatch.Run(DoLazyLoadInitialItems);

        async Task DoLazyLoadInitialItems()
        {
            var visibleHeight = FindParent<ScrollView>()?.ActualHeight ?? Page?.ActualHeight ?? Root.ActualHeight;
            visibleHeight -= ActualY;

            var startIndex = 0;
            VisibleItems = 0;

            GridHeight = ManagedChildren.Sum(i => i.CalculateTotalHeight());

            while (GridHeight < visibleHeight && startIndex < DataSource.Count())
            {
                var item = await AddItem(DataSource[startIndex]);
                startIndex++;
                VisibleItems++;

                if (VisibleItems % Columns == 0)
                    GridHeight += item.ActualHeight;
            }

            if (DataSource.Count == startIndex && ExactColumns)
                await EnsureFullColumns();
        }

        /// <summary>
        /// Returns whether it successfully added one.
        /// </summary>
        async Task<bool> LazyLoadMore()
        {
            TSource next;
            lock (DataSourceSyncLock) next = DataSource.Skip(VisibleItems).FirstOrDefault();

            if (next == null)
            {
                if (ExactColumns) await EnsureFullColumns();
                return false;
            }

            VisibleItems++;
            var item = CreateItem(next);
            await Add(item);

            if (VisibleItems % Columns == 0)
                GridHeight += item.ActualHeight;

            return true;
        }
    }
}