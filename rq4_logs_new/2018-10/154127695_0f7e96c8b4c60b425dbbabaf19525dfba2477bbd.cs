using System;
using System.Data;
using System.Data.SqlClient;

namespace Bango.SqlImport
{
    public class BulkCopyUtility : IBulkCopyUtility
    {
        private readonly string connectionString;

        public BulkCopyUtility(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public void BulkCopy(string tableName, IDataReader dataReader)
        {
            this.BulkCopy(tableName, dataReader, null);
        }

        public void BulkCopy(string tableName, IDataReader dataReader, Action<SqlBulkCopy> configureSqlBulkCopy)
        {
            using (SqlConnection dbConnection = new SqlConnection(connectionString))
            {
                dbConnection.Open();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(dbConnection))
                {
                    bulkCopy.BatchSize = 3000;
                    bulkCopy.EnableStreaming = true;
                    bulkCopy.DestinationTableName = tableName;

                    //This will ensure mapping based on names rather than column position
                    foreach (DataColumn column in dataReader.GetSchemaTable().Columns)
                    {
                        bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                    }

                    //If additional, custom configuration is required, invoke the action
                    configureSqlBulkCopy?.Invoke(bulkCopy);

                    try
                    {
                        // Write from the source to the destination.
                        bulkCopy.WriteToServer(dataReader);
                    }
                    finally
                    {
                        dataReader.Close();
                    }
                }
            }
        }
    }
}