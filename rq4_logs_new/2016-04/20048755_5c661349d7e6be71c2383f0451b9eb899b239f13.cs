using System;

namespace xpf.Scripting.SQLServer
{
    /// <summary>
    /// This exception class wraps a SqlException adding the number of retries that were attempted.
    /// </summary>
    internal class SqlRetryException : Exception
    {
        public SqlRetryException(int retryCount, Exception innerException) : base("", innerException)
        {
            this.RetryCount = retryCount;
        }

        public int RetryCount { get; private set; }
    }
}