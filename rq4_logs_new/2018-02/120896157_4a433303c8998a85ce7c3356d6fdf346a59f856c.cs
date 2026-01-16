using Catel.Collections;
using Catel.IoC;
using Catel.MVVM;
using IF.WPF.Infragistics.Persistence.DockManager.Interfaces;
using IF.WPF.Infragistics.Persistence.Extensions;
using Infragistics.Windows.DockManager;
using Infragistics.Windows.DockManager.Events;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Interactivity;

namespace IF.WPF.Infragistics.Persistence.DockManager.Behaviors
{
    public class TabGroupPaneItemsSourceBehavior : Behavior<XamDockManager>
    {
        private readonly IViewLocator viewLocator;

        private IDictionary<InitialPaneLocation, TabGroupPane> tabGroupPanesMapping;

        private TabGroupPane GetPane(string name)
        {
            var result = AssociatedObject.FindChildrenByType<TabGroupPane>();

            return result as TabGroupPane;
        }

        public static readonly DependencyProperty HeaderTemplateProperty = DependencyProperty.Register("HeaderTemplate", typeof(DataTemplate), typeof(TabGroupPaneItemsSourceBehavior), new PropertyMetadata(null));
        public DataTemplate HeaderTemplate
        {
            get { return (DataTemplate)GetValue(HeaderTemplateProperty); }
            set { SetValue(HeaderTemplateProperty, value); }
        }

        public static readonly DependencyProperty ItemsSourceProperty = DependencyProperty.Register("ItemsSource", typeof(IList), typeof(TabGroupPaneItemsSourceBehavior), new PropertyMetadata(null, new PropertyChangedCallback(OnItemsSourcePropertyChanged)));
        public IList ItemsSource
        {
            get { return (IList)GetValue(ItemsSourceProperty); }
            set { SetValue(ItemsSourceProperty, value); }
        }

        private static void OnItemsSourcePropertyChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            TabGroupPaneItemsSourceBehavior behavior = d as TabGroupPaneItemsSourceBehavior;
            if (behavior != null)
                behavior.OnItemsSourcePropertyChanged((IList)e.OldValue, (IList)e.NewValue);
        }

        void OnItemsSourcePropertyChanged(IList oldValue, IList newValue)
        {
            tabGroupPanesMapping.ForEach(x => x.Value.Items.Clear());

            if (oldValue != null)
            {
                var oldCollectionChanged = oldValue as INotifyCollectionChanged;
                if (oldCollectionChanged != null)
                    oldCollectionChanged.CollectionChanged -= CollectionChanged_CollectionChanged;
            }

            if (newValue != null)
            {
                var collectionChanged = newValue as INotifyCollectionChanged;
                if (collectionChanged != null)
                    collectionChanged.CollectionChanged += CollectionChanged_CollectionChanged;

                foreach (var item in newValue)
                {
                    ContentPane contentPane = PrepareContainerForItem(item);

                    AddItemToCorrectPane(item, contentPane);
                }
            }
        }

        private void AddItemToCorrectPane(object item, ContentPane contentPane)
        {
            var intialDockPosition = item as IInitialPosition;

            if (intialDockPosition != null)
            {
                if (tabGroupPanesMapping.ContainsKey(intialDockPosition.InitialPaneLocation))
                {
                    tabGroupPanesMapping[intialDockPosition.InitialPaneLocation].Items.Add(contentPane);
                }
            }
            else
            {
                tabGroupPanesMapping[InitialPaneLocation.DockedTop].Items.Add(contentPane);
            }
        }

        void CollectionChanged_CollectionChanged(object sender, NotifyCollectionChangedEventArgs e)
        {

            if (e.Action == NotifyCollectionChangedAction.Remove)
            {
                IEnumerable<ContentPane> contentPanes = XamDockManager.GetDockManager(AssociatedObject).GetPanes(PaneNavigationOrder.VisibleOrder);
                foreach (ContentPane contentPane in contentPanes)
                {
                    var dc = contentPane.DataContext;
                    if (dc != null && e.OldItems.Contains(dc))
                    {
                        contentPane.ExecuteCommand(ContentPaneCommands.Close);
                    }
                }
            }
            else if (e.Action == NotifyCollectionChangedAction.Add)
            {
                foreach (var item in e.NewItems)
                {
                    ContentPane contentPane = PrepareContainerForItem(item);

                    AddItemToCorrectPane(item, contentPane);
                }
            }
            else if (e.Action == NotifyCollectionChangedAction.Reset)
            {
                tabGroupPanesMapping.ForEach(x => x.Value.Items.Clear());
            }
        }

        protected ContentPane PrepareContainerForItem(object item)
        {
            ContentPane container = new ContentPane();
            var view = CreateViewContent(item) as FrameworkElement;

            container.SetValue(FrameworkElement.NameProperty, "n" + Guid.NewGuid().ToString().Split('-')[0]);

            container.Content = view;
            container.DataContext = item;

            if (HeaderTemplate != null)
            {
                //   container.HeaderTemplate = HeaderTemplate;

                var headerTemplate = Application.Current.Resources["PaneHeaderTemplate"] as DataTemplate;

                Binding binding = new Binding { Source = headerTemplate };
                Binding bindingViewModel = new Binding { Source = item };

                BindingOperations.SetBinding(container, HeaderedContentControl.HeaderTemplateProperty, binding);
                BindingOperations.SetBinding(container, ContentPane.TabHeaderTemplateProperty, binding);

                BindingOperations.SetBinding(container, HeaderedContentControl.HeaderProperty, bindingViewModel);
                BindingOperations.SetBinding(container, ContentPane.TabHeaderProperty, bindingViewModel);
            }

            Binding persistenceBagBinding = new Binding { Source = item, Path = new PropertyPath("PersistenceBag") };
            BindingOperations.SetBinding(container, ContentPane.SerializationIdProperty, persistenceBagBinding);

            container.CloseAction = PaneCloseAction.RemovePane;
            container.Closed += Container_Closed;

            CreateBindings(item, container);

            return container;
        }

        private void Container_Closed(object sender, PaneClosedEventArgs e)
        {
            ContentPane contentPane = sender as ContentPane;
            if (contentPane != null)
            {
                contentPane.Closed -= Container_Closed; //no memory leaks

                var item = contentPane.DataContext;

                //if (ItemsSource != null && ItemsSource.Contains(item))
                //    ItemsSource.Remove(item);

                RemoveBindings(contentPane);
            }
        }

        private void CreateBindings(object item, ContentPane contentPane)
        {
            //if (!String.IsNullOrWhiteSpace(HeaderMemberPath))
            //{
            //    Binding headerBinding = new Binding(HeaderMemberPath);
            //    headerBinding.Source = item;
            //    contentPane.SetBinding(ContentPane.HeaderProperty, headerBinding);
            //}
        }

        private void RemoveBindings(ContentPane contentPane)
        {
            contentPane.ClearValue(ContentPane.HeaderProperty);
        }

        public TabGroupPaneItemsSourceBehavior()
        {
            viewLocator = ServiceLocator.Default.ResolveType<IViewLocator>();
        }

        protected override void OnAttached()
        {
            base.OnAttached();

            this.AssociatedObject.Loaded += AssociatedObject_Loaded;

        }

        private void AssociatedObject_Loaded(object sender, RoutedEventArgs e)
        {
            var panes = AssociatedObject.FindChildrenByType<SplitPane>();
            tabGroupPanesMapping = panes.ToDictionary(x => (InitialPaneLocation)x.GetValue(XamDockManager.InitialLocationProperty), x => x.FindChildByType<TabGroupPane>());
        }

        private static object CreateViewContent(object viewModel)
        {
            IViewLocator viewLocator = ServiceLocator.Default.ResolveType<IViewLocator>();
            Type viewType = viewLocator.ResolveView(viewModel.GetType());
            FrameworkElement view = ViewHelper.ConstructViewWithViewModel(viewType, viewModel);

            return view;
        }
    }
}