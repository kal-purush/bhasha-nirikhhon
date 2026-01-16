using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading.Tasks;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultBulkStoreDataFeature : IBulkStoreDataFeature
    {
        public virtual async Task BulkTableUpdate(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName,
            IDictionary<string, object> data,
            int threshold,
            IBulkSqlClient client,
            Func<Task<IConnectorConnection>> connectionFactory,
            ILogger logger)
        {
            var table = GetDataTable(executionContext, containerName, data);
            CacheDataTableRow(data, table);

            if(table.Rows.Count >= threshold)
            {
                await FlushTable(executionContext, connectionFactory, client, containerName, table, logger);
            }            
        }

        private static void CacheDataTableRow(IDictionary<string, object> data, DataTable table)
        {
            var row = table.NewRow();
            foreach (var item in data)
            {
                row[item.Key.SqlSanitize()] = item.Value;
            }

            table.Rows.Add(row);
        }

        private async Task FlushTable(
            ExecutionContext executionContext,
            Func<Task<IConnectorConnection>> connectionFactory,
            IBulkSqlClient client,
            string containerName,
            DataTable table,
            ILogger logger)
        {
            var dataTableCacheName = GetDataTableCacheName(containerName);
            executionContext.ApplicationContext.System.Cache.RemoveItem(dataTableCacheName);

            var connection = await connectionFactory();

            var sw = new Stopwatch();
            sw.Start();
            await client.ExecuteBulkAsync(connection, table, containerName);
            logger.LogDebug($"Stream StoreData BulkInsert {table.Rows.Count} rows - {sw.ElapsedMilliseconds}ms");
        }

        private DataTable GetDataTable(ExecutionContext executionContext, string containerName, IDictionary<string, object> data)
        {
            var dataTableCacheName = GetDataTableCacheName(containerName);

            return executionContext.ApplicationContext.System.Cache.GetItem(dataTableCacheName, () =>
            {
                var table = new DataTable(containerName);
                foreach (var col in data)
                {
                    table.Columns.Add(col.Key.SqlSanitize(), typeof(string));
                }

                return table;
            });
        }

        private static string GetDataTableCacheName(string containerName)
        {
            return $"Stream_cache_{containerName.SqlSanitize()}";
        }
    }
}