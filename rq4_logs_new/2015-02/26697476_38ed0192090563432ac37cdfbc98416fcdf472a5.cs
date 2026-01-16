using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;

namespace FusionMVVM.Common
{
    public static class DependencyObjectExtensions
    {
        /// <summary>
        /// Finds the logical children of type T.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="parent"></param>
        /// <returns></returns>
        public static IEnumerable<T> FindLogicalChildren<T>(this DependencyObject parent) where T : DependencyObject
        {
            if (parent == null) throw new ArgumentNullException("parent");

            // Gets all DependencyObjects from the parent.
            var children = LogicalTreeHelper.GetChildren(parent).OfType<DependencyObject>().ToList();

            foreach (var child in children)
            {
                if (child is T)
                {
                    yield return (T)child;
                }

                foreach (var childOfChild in child.FindLogicalChildren<T>())
                {
                    yield return childOfChild;
                }
            }
        }
    }
}