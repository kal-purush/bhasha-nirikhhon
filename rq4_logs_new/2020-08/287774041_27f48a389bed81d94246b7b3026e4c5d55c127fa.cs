using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Windows.Data;
using System.Windows.Threading;

namespace Toolkit.Observable
{
    public class ObservableObjectCollection<T> : ObservableObject, IObservableObjectCollection<T> where T : class, IObservable
    {
        #region Fields
        private readonly ObservableCollection<T> _baseCollection;
        private readonly object _collectionLock;
        private string _filterText;
        #endregion

        #region Events
        public event SyncableCollectionItemHandler ItemChanged;
        public event SyncableCollectionItemHandler ItemAdded;
        public event SyncableCollectionItemHandler ItemRemoved;
        public event SyncableCollectionItemHandler SelectedItemChanged;
        public event SyncableCollectionSelectionHandler SelectedItemChanging;
        #endregion

        #region Eventhandler
        public delegate void SyncableCollectionItemHandler(T item);
        public delegate bool SyncableCollectionSelectionHandler(T item, SelectionChangingEventArgs e);
        #endregion

        #region Constructor
        public ObservableObjectCollection(ObservableCollection<T> collection = null)
        {
            _baseCollection = collection ?? new ObservableCollection<T>();
            _collectionLock = new object();

            Dispatcher
                .CurrentDispatcher
                .Invoke(() => BindingOperations.EnableCollectionSynchronization(_baseCollection, _collectionLock));

            foreach (var item in _baseCollection)
            {
                item.PropertyChanged += (s, o) => ItemChanged?.Invoke(item);
            }

            CollectionView = CollectionViewSource.GetDefaultView(_baseCollection);

            ItemAdded += item => UpdateProperties();
            ItemRemoved += item => UpdateProperties();

            CollectionView.Filter = Filter;
        }
        #endregion

        #region Local methods
        private bool Filter(object param)
        {
            if (string.IsNullOrEmpty(FilterText))
            {
                return true;
            }

            if (param is T item)
            {
                return CollectionViewFilter?.Invoke(item) ?? true;
            }
            else
            {
                return false;
            }
        }

        private void UpdateProperties()
        {
            NotifyPropertyChanged(nameof(IsEmpty));
            NotifyPropertyChanged(nameof(Count));
        }
        #endregion

        #region Implementation : ISyncableObjectCollection
        public ICollectionView CollectionView { get; }
        public T SelectedItem
        {
            get => (T)CollectionView.CurrentItem;
            set
            {
                var selectionEventArgs = new SelectionChangingEventArgs();
                SelectedItemChanging?.Invoke(SelectedItem, selectionEventArgs);
                if (selectionEventArgs.CancelSelectionChange)
                {
                    return;
                }

                if (!EqualityComparer<T>.Default.Equals(value, default))
                {
                    CollectionView.MoveCurrentTo(value);
                    NotifyPropertyChanged();
                    SelectedItemChanged?.Invoke(SelectedItem);
                }
            }
        }

        public Predicate<T> CollectionViewFilter { get; set; }
        public string FilterText
        {
            get => _filterText;
            set
            {
                SetAndNotify(ref _filterText, value);
                CollectionView?.Refresh();
            }
        }
        public bool IsEmpty { get => _baseCollection.Count == 0; }
        #endregion

        #region Implementation : ICollection
        public int Count { get => _baseCollection.Count; }
        public bool IsReadOnly { get => false; }

        public void Add(T item)
        {
            if (item is null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            item.PropertyChanged += (s, o) => ItemChanged?.Invoke(item);

            lock (_collectionLock)
            {
                _baseCollection.Add(item);
            }

            SelectedItem = item;
            ItemAdded?.Invoke(item);
        }

        public void Clear()
        {
            lock (_collectionLock)
            {
                _baseCollection.Clear();
            }
        }
        public bool Contains(T item)
        {
            lock (_collectionLock)
            {
                return _baseCollection.Contains(item);
            }
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            lock (_collectionLock)
            {
                _baseCollection.CopyTo(array, arrayIndex);
            }
        }

        public bool Remove(T item)
        {
            if (item is null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            lock (_collectionLock)
            {
                if (!_baseCollection.Contains(item))
                {
                    return false;
                }
                else
                {
                    _baseCollection.Remove(item);
                }
            }

            item.PropertyChanged -= (s, o) => ItemChanged?.Invoke(item);
            ItemRemoved?.Invoke(item);

            return true;
        }
        #endregion

        #region Implementation : IEnumerable
        public IEnumerator<T> GetEnumerator()
        {
            return _baseCollection.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
        #endregion
    }
}