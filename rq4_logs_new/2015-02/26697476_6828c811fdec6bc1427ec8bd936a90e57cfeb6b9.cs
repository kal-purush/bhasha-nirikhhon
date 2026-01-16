using System;
using System.Windows;

namespace FusionMVVM.Common
{
    public interface IWindowInitiator
    {
        /// <summary>
        /// Initializes and returns a new Window, with the available
        /// DataContext and owner.
        /// </summary>
        /// <param name="windowType"></param>
        /// <param name="dataContext"></param>
        /// <param name="ownerWindow"></param>
        /// <returns></returns>
        Window Initialize(Type windowType, object dataContext, Window ownerWindow);
    }
}