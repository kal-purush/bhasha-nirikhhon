using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;


namespace QuestStore.Infrastructure.Data
{
    public static class EFCoreExtensions
    {
        public static IQueryable<T> Include<T>(this IQueryable<T> dataSource,
            IEnumerable<string> navigationPropertiesPaths) where T : class =>
            navigationPropertiesPaths.Aggregate(dataSource, (query, path) => query.Include(path));

        public static IEnumerable<string> GetAllPaths(this DbContext context, Type clrType, int maxDepth = 2)
        {
            if (maxDepth <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxDepth), "maxDepth must be greater than zero.");
            }

            var allPaths = new List<string>();
            var path = new Stack<string>(); //Because string is reference type we must use stack to build path by recursion;

            var entityType =  context.Model.FindEntityType(clrType);
            var includedNavigations = new HashSet<string>(); //It marks visited nodes.

            foreach (var navigation in entityType.GetNavigations())
            {
                if (includedNavigations.Add(navigation.Name))
                {
                    path.Push(navigation.Name);
                    NavigationsDfs(navigation, path, allPaths, includedNavigations, maxDepth - 1);
                }
            }

            return allPaths;
        }

        private static void NavigationsDfs(INavigation initial, Stack<string> path,
            List<string> allPaths, HashSet<string> includedNavigations, int depth)
        {
            var inverseNavigation = initial.FindInverse();
            if (inverseNavigation != null)
            {
                includedNavigations.Add(inverseNavigation.Name);
            }

            var navigations = initial.GetTargetType().GetNavigations().ToList();
            if (navigations == null || navigations.All(n => includedNavigations.Contains(n.Name)) ||
                depth <= 0)
            {
                allPaths.Add(string.Join(".", path.Reverse()));
                path.Pop();
                return;
            }

            foreach (var navigation in navigations)
            {
                if (includedNavigations.Add(navigation.Name))
                {
                    path.Push(navigation.Name);
                    NavigationsDfs(navigation, path, allPaths, includedNavigations, depth-1);
                }
            }
        }
    }
}