using System.Collections.Generic;
using System.Linq;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class Container
    {
        private static readonly ConnectionDataType[] _codeEdgeColumns = new[]
        {
            new ConnectionDataType { Name = "OriginEntityCode".SqlSanitize(), Type = VocabularyKeyDataType.Text },
            new ConnectionDataType { Name =  "Code".SqlSanitize(), Type = VocabularyKeyDataType.Text }
        };

        public Container(string containerName)
        {
            PrimaryTable = containerName.SqlSanitize();
            Tables = new Dictionary<string, Table>
            {
                ["Codes"] = new Table($"{containerName}Codes", _codeEdgeColumns, _codeEdgeColumns.Select(x => x.Name)),
                ["Edges"] = new Table($"{containerName}Edges", _codeEdgeColumns, _codeEdgeColumns.Select(x => x.Name)),
            };
        }

        public string PrimaryTable { get; }

        public IReadOnlyDictionary<string, Table> Tables { get; }

        public class Table
        {
            public Table(
                string name,
                IEnumerable<ConnectionDataType> columns,
                IEnumerable<string> keys)
            {
                Name = name.SqlSanitize();
                Columns = new List<ConnectionDataType>(columns).AsReadOnly();
                Keys = new List<string>(keys).AsReadOnly();
            }

            public string Name { get; }

            public IReadOnlyList<ConnectionDataType> Columns { get; }

            public IReadOnlyList<string> Keys { get; }
        }
    }
}