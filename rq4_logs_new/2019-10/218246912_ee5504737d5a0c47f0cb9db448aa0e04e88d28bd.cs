using System;

namespace ParseTo.Exceptions
{
    public class ParseToNotImplementedException<T> : Exception
    {
        public ParseToNotImplementedException()
        : base($"The type of { typeof(T).Name } has not got an implemented parse handler inheriting IParse<{ typeof(T).Name }>")
        { }
    }
}