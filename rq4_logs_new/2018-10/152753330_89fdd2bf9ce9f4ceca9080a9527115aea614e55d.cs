using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace StringTable
{
    public static class Extensions
    {
        public static string Wrap(this IEnumerable<string> text, string sep) =>
            $"{sep} {string.Join($" {sep} ", text)} {sep}";

        public static IEnumerable<IEnumerable<T>> Transpose<T>(this IEnumerable<IEnumerable<T>> matrix) => matrix
            .Any(row => row.Count() > 0)
                ? new[] { matrix.Select(row => row.FirstOrDefault()) }
                    .Concat(matrix.Select(row => row.Skip(1)).Transpose())
                : Enumerable.Empty<IEnumerable<T>>();

        private static int TablePropertyOrder(this PropertyInfo propInfo) => propInfo
            .GetCustomAttribute<TableColumnAttribute>(inherit: true)
            ?.Order ?? int.MaxValue;

        private static bool? TablePropertyIncluded(this PropertyInfo propInfo)
        {
            var attribute = propInfo.GetCustomAttribute<TableColumnAttribute>(inherit: true);

            return !attribute?.Exclude ?? attribute?.Include;
        }

        private static bool TableDefaultIncluded(this Type type) => !type
            .GetCustomAttribute<TableAttribute>()
            ?.ExcludeByDefault ?? true;

        private static IOrderedEnumerable<PropertyInfo> GetObjectProperties(this Type type) => type
            .GetProperties()
            .Where(prop => prop.TablePropertyIncluded() ?? type.TableDefaultIncluded())
            .OrderBy(prop => prop.TablePropertyOrder());

        public static StringTable ToTable<T>(this IEnumerable<T> enumerable, bool derived = false)
        {
            if (enumerable.Count() == 0)
                throw new ArgumentException("Empty enumerable");

            Regex splitCamelCase = new Regex(@"(?<=[a-z])([A-Z])|(?<=[A-Z])([A-Z][a-z])");

            IEnumerable<string> columns = enumerable
                .Select(item => GetObjectProperties(derived
                        ? item.GetType()
                        : typeof(T)
                    )
                    .Select(prop => prop.Name)
                    .Select(name => splitCamelCase.Replace(name, " $1$2"))
                )
                .Transpose()
                .Select(columnOfProps =>
                    columnOfProps.All(cell => cell == columnOfProps.First())
                        ? columnOfProps.First()
                        : columnOfProps.Aggregate((cell1, cell2) =>
                            cell2 != null
                                ? $"{cell1}/{cell2}"
                                : cell1
                        )
                );

            if (columns.Count() == 0)
                throw new ArgumentException("Found no properties");

            StringTable table = new StringTable();

            table.Columns.AddRange(columns
                .Select(col => new DataColumn(col))
                .ToArray()
            );

            enumerable.Select(item =>
                GetObjectProperties(derived
                    ? item.GetType()
                    : typeof(T)
                )
                .Select(prop => prop.GetValue(item))
                .Select(value => value.ToString())
            )
            .ToList()
            .ForEach(row => table.Rows.Add(row.ToArray()));

            return table;
        }
    }
}