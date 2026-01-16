using System.ComponentModel;

namespace Toolkit.Observable
{
    public interface IObservable : INotifyPropertyChanged
    {
        ///
        /// <summary>
        /// Set the <paramref name="newValue"/> to the <paramref name="property"/>
        /// and notify others of the change by invoking the <cref name="PropertyChanged"/> event.
        /// </summary>
        ///
        /// <param name="property">Reference to the backing field of the property.</param>
        /// <param name="newValue">The new value.</param>
        /// <param name="propertyName">
        /// Name of the changed property. This can be omitted, since the implementation
        /// should resolve it via the compiler name service./>.
        /// </param>
        ///
        void SetAndNotify<T>(ref T property, T newValue, string propertyName);
    }
}