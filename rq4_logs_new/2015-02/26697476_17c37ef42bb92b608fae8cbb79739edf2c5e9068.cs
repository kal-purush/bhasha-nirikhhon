using System;
using System.Collections.Generic;
using System.Linq;

namespace FusionMVVM.Common
{
    public class EndsWithTypeFilter : ITypeFilter
    {
        private readonly string _text;
        private readonly bool _caseSensitive;

        /// <summary>
        /// Initializes a new instance of the EndsWithTypeFilter class.
        /// </summary>
        /// <param name="text"></param>
        /// <param name="caseSensitive"></param>
        public EndsWithTypeFilter(string text, bool caseSensitive = true)
        {
            if (text == null) throw new ArgumentNullException("text");

            _text = text;
            _caseSensitive = caseSensitive;
        }

        /// <summary>
        /// Applies the filter and returns the filtered collection of types,
        /// ending with the provided text.
        /// </summary>
        /// <param name="enumerable"></param>
        /// <returns></returns>
        public IEnumerable<Type> ApplyFilter(IEnumerable<Type> enumerable)
        {
            var comparison = _caseSensitive ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase;
            var filteredTypes = from type in enumerable
                                where type.FullName != null && (type.IsClass && type.FullName.EndsWith(_text, comparison))
                                select type;

            return filteredTypes;
        }
    }
}