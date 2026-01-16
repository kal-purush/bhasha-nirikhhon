using System;
using System.Collections.Generic;
using System.Linq;

namespace Contractor.Core.Helpers
{
    internal static class DependencyCycleHelper
    {
        public static IEnumerable<T> FindCycle<T>(
            this IEnumerable<T> source,
            Func<T, IEnumerable<T>> dependencies)
        {
            foreach (var item in source)
            { 
                var cycle = FindCycleStartingAtItem(item, item, new List<T>(), new List<T>(), dependencies);
                if (cycle != null)
                    return cycle;
            }

            return null;
        }

        private static IEnumerable<T> FindCycleStartingAtItem<T>(
            T startItem,
            T item,
            IEnumerable<T> visited,
            IEnumerable<T> cycle,
            Func<T, IEnumerable<T>> getDependenciesFunction)
        {
            if (visited.Contains(item))
            {
                return cycle.Append(item);
            }

            foreach (var dependency in getDependenciesFunction(item))
            {
                var cycleResult = FindCycleStartingAtItem(startItem, dependency, visited.Append(item), cycle.Append(item), getDependenciesFunction);
                if (cycleResult != null && cycleResult.LastOrDefault().Equals(startItem))
                    return cycleResult;
            }

            return null;
        }
    }
}