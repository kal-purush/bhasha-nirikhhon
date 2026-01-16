using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;

namespace FotoSortierer_v2.Helper.Adapter.Interfaces
{
    /// <summary>
    /// Represents a dynamic data collection that provides notifications when items get added, removed, or when the whole list is refreshed.
    /// Provides additionally the possibility to add collections in one step.
    /// </summary>
    /// <typeparam name="T">Type of elements in collection.</typeparam>
    public interface IObservableCollectionAdapter<T> : IList<T>, ICollection<T>, IEnumerable<T>, IEnumerable, IList, ICollection, IReadOnlyList<T>, IReadOnlyCollection<T>, INotifyCollectionChanged
    {
        void AddRange(IEnumerable<T> collection);
    }
}