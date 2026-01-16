using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NetSort.Sorting
{
    internal abstract class BaseSorter<TIn, TOut, TType> 
        where TIn : IEnumerable<TType>
        where TOut : IEnumerable<TType>
        where TType : class
    {
        protected abstract TOut DoSortAsc(TIn items, IEnumerable<SortOperationMetadata> metadata);
        protected abstract TOut DoSortDesc(TIn items, IEnumerable<SortOperationMetadata> metadata);
       
        public TOut Sort(TIn items, string key, char delimiter, SortDirection? direction)
        {
            var metadata = SortOperationMetadataFinder.Find<TType>(key, delimiter, direction);
            if (!metadata.Any())
            {
                var error = $"A 'sortable attribute' could not be found.\n Key: {key} \nType: {typeof(TType).Name}.";
                throw new ArgumentOutOfRangeException(nameof(key), error);
            }

            return DoSort(items, metadata);
        }

        public TOut SortWithStringDirection(TIn items, string key, char delimiter, string direction)
        {
            var directionEnum = ParseDirection(direction);
            return Sort(items, key, delimiter, directionEnum);
        }

        protected static object GetNestedValue(IEnumerable<SortOperationMetadata> metadatas, object rootObj)
        {
            foreach (var metadata in metadatas)
            {
                rootObj = metadata.ToSortBy.GetValue(rootObj);
            }

            return rootObj;
        }

        private SortDirection ParseDirection(string val)
        {
            SortDirection parsedDirection;
            var wasParseSuccessful = Enum.TryParse(val, true, out parsedDirection);
            if (wasParseSuccessful)
            {
                return parsedDirection;
            }

            throw new ArgumentOutOfRangeException("direction", $"{val} is not a valid 'SortDirection'");
        }

        private TOut DoSort(TIn items, IEnumerable<SortOperationMetadata> metadata)
        {
            if (items == null)
            {
                return default(TOut);
            }

            if (metadata.Last().Direction == SortDirection.Asc)
            {
                return DoSortAsc(items, metadata);
            }
            else
            {
                return DoSortDesc(items, metadata);
            }
        }                        
    }
}