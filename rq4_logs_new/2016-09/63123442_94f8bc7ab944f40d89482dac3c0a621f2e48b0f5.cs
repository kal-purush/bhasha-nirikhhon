using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Windows;

namespace InteractionWpf
{
    public static class DispatcherHelper
    {
        static readonly DependencyObject PseudoElement = new DependencyObject();

        public static bool IsInDesignMode
        {
            get { return DesignerProperties.GetIsInDesignMode(PseudoElement); }
        }

        public static bool IsInUIThread
        {
            get { return !Thread.CurrentThread.IsBackground; }
        }
    }
}