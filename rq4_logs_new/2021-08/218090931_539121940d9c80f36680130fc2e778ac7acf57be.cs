using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Dapper.Wrappers.Generators
{
    public interface IMetadataGenerator
    {
        QueryOperationMetadata GetDefaultOperation<T>(string name, string baseQueryString);

        QueryOperationMetadata GetOperation(string name, string baseQueryString,
            IEnumerable<QueryParameterMetadata> parameters);

        MergeOperationMetadata GetDefaultMergeOperation<T>(string name, string baseQueryString, string referencedColumn);

        MergeOperationMetadata GetMergeOperation(string name, string baseQueryString, string referencedColumn,
            IEnumerable<QueryParameterMetadata> parameters);

        QueryParameterMetadata GetParameter<T>(string name, object defaultValue = null, bool hasDefault = false);

        QueryParameterMetadata GetParameter(string name, DbType? paramType, object defaultValue = null,
            bool hasDefault = false);
    }
}