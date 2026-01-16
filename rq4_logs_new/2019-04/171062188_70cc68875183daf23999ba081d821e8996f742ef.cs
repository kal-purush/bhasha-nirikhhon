using System;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Threading;

namespace WordTutor.Desktop
{
    public interface IViewModel<out T>
    {
        T Model { get; }
    }

    public abstract class ViewModelBase : INotifyPropertyChanged
    {
        // Captured synchronization context so we always dispatch events on the UX thread
        private readonly SynchronizationContext _synchronizationContext;

        public event PropertyChangedEventHandler PropertyChanged;

        protected ViewModelBase()
        {
            // We always want our PropertyChanged event to be fired on the UX thread
            _synchronizationContext = SynchronizationContext.Current;
        }

        protected void UpdateProperty(
            ref int member,
            int newValue,
            [CallerMemberName] string property = null)
        {
            if (member == newValue)
            {
                return;
            }

            member = newValue;
            OnPropertyChanged(property);
        }

        protected void UpdateProperty<E>(
            ref E member,
            E newValue,
            [CallerMemberName] string property = null)
            where E : Enum
        {
            if (Equals(member, newValue))
            {
                return;
            }

            member = newValue;
            OnPropertyChanged(property);
        }

        protected void UpdateProperty(
            ref string member,
            string newValue,
            [CallerMemberName] string property = null)
        {
            if (member == newValue)
            {
                return;
            }

            member = newValue;
            OnPropertyChanged(property);
        }

        protected void UpdateProperty(
            ref DateTimeOffset member,
            DateTimeOffset newValue,
            [CallerMemberName] string property = null)
        {
            if (member == newValue)
            {
                return;
            }

            member = newValue;
            OnPropertyChanged(property);
        }

        private void OnPropertyChanged(string property)
        {
            if (SynchronizationContext.Current == _synchronizationContext
                || _synchronizationContext == null)
            {
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(property));
            }
            else
            {
                _synchronizationContext.Send(
                      _ => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(property)),
                      state: null);
            }
        }
    }

    public abstract class ViewModelBase<T> : ViewModelBase, IViewModel<T>
    {
        // Reference to our current model
        private T _model;

        /// <summary>
        /// Gets or sets a reference to the current model held by this ViewModel
        /// </summary>
        public T Model
        {
            get => _model;
            set
            {
                _model = value;
                ModelUpdated(value);
            }
        }

        /// <summary>
        /// Update properties when the Model is changed
        /// </summary>
        /// <param name="model"></param>
        protected abstract void ModelUpdated(T model);
    }
}