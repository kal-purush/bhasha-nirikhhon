using Azure;
using Azure.Data.Tables;
using System;

namespace Website.Function.Email.TableStorage.Entities
{
    public class ExceptionDetails : ITableEntity
    {
        private string _partitionKey { get; set; } = String.Empty;

        private string _rowKey { get; set; } = String.Empty;

        private DateTimeOffset? _timestamp { get; set; }

        private ETag _eTag { get; set; }

        // Recipient Email Address we are messaging
        public string PartitionKey { get => _partitionKey; set => _partitionKey = value; }

        // Incoming Queue Message String. Always require a RowKey
        public string RowKey { get => _rowKey; set => _rowKey = value; }

        public DateTimeOffset? Timestamp { get => _timestamp; set => _timestamp = value; }
        
        public ETag ETag { get => _eTag; set => _eTag = value; }

        public string ExceptionMessage { get; set; } = String.Empty;
    }
}