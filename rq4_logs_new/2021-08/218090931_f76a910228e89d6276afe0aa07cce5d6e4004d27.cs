// © 2021 by Benjamin Skeen
// Licensed to be used under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Dapper.Wrappers.Formatters;

namespace Dapper.Wrappers.Generators
{
    public abstract class GenericDeleteQueryGenerator<T> : DeleteQueryGenerator, IGenericDeleteQueryGenerator<T>
    {
        protected GenericDeleteQueryGenerator(IQueryFormatter queryFormatter) : base(queryFormatter)
        {
        }

        public abstract IEnumerable<QueryOperation> GetDeleteQueryOperations(T operationSource);
    }
}