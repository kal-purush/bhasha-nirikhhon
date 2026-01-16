using System;
using System.Threading;
using System.ComponentModel;
using System.Linq.Expressions;

namespace SimpleMvvmToolkit
{
    /// <summary>
    /// Consolidated data binding helper methods
    /// </summary>
    public static class BindingHelper
    {
        /// <summary>
        /// Defined as an extension method for use by subclasses.
        /// Usage: this.NotifyPropertyChanged(m => m.PropertyName, propertyChanged);
        /// </summary>
        /// <typeparam name="TModel">ViewModel or model property type</typeparam>
        /// <typeparam name="TResult">Property result type</typeparam>
        /// <param name="model">ViewModel or model</param>
        /// <param name="property">ViewModel or model property</param>
        /// <param name="propertyChanged">PropertyChanged event</param>
        /// <param name="dispatcher">Dispatcher for marshalling call to UI thread</param>
        public static void NotifyPropertyChanged<TModel, TResult>
            (this TModel model, Expression<Func<TModel, TResult>> property,
            PropertyChangedEventHandler propertyChanged, IDispatcher dispatcher)
        {
            // Convert expression to a property name
            string propertyName = ((MemberExpression)property.Body).Member.Name;

            // Fire notify property changed event
            InternalNotifyPropertyChanged(propertyName, model, propertyChanged, dispatcher);
        }

        /// <summary>
        /// Fire PropertyChanged event for two-way data binding.
        /// </summary>
        /// <typeparam name="TModel">ViewModel or model property type</typeparam>
        /// <typeparam name="TResult">Property result type</typeparam>
        /// <param name="property">ViewModel or model property</param>
        /// <param name="sender">Instance of class firing the event</param>
        /// <param name="propertyChanged">PropertyChanged event</param>
        /// <param name="dispatcher">Dispatcher for marshalling call to UI thread</param>
        public static void NotifyPropertyChanged<TModel, TResult>
            (Expression<Func<TModel, TResult>> property,
            object sender, PropertyChangedEventHandler propertyChanged,
            IDispatcher dispatcher)
        {
            // Convert expression to a property name
            string propertyName = ((MemberExpression)property.Body).Member.Name;

            // Fire notify property changed event
            InternalNotifyPropertyChanged(propertyName, sender, propertyChanged, dispatcher);
        }

        //public static void InternalNotifyPropertyChanged(string propertyName,
        //    object sender, PropertyChangedEventHandler propertyChanged)
        //{
        //    InternalNotifyPropertyChanged(propertyName, sender, propertyChanged,
        //        SyncContext.Current);
        //}

        /// <summary>
        /// Fire PropertyChanged event for two-way data binding.
        /// </summary>
        /// <param name="propertyName">ViewModel or model property</param>
        /// <param name="sender">Instance of class firing the event</param>
        /// <param name="propertyChanged">PropertyChanged event</param>
        /// <param name="dispatcher">Dispatcher for marshalling call to UI thread</param>
        public static void InternalNotifyPropertyChanged(string propertyName,
            object sender, PropertyChangedEventHandler propertyChanged, IDispatcher dispatcher)
        {
            if (propertyChanged != null)
            {
                // Fire the event on the UI thread
                if (dispatcher == null || dispatcher.CheckAccess())
                {
                    propertyChanged(sender, new PropertyChangedEventArgs(propertyName));

                }
                else
                {
                    dispatcher.BeginInvoke(() => propertyChanged
                        (sender, new PropertyChangedEventArgs(propertyName)));
                }
            }
        }
    }
}