using System.Collections;
using System.Collections.Generic;
using System.IO;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using CsvHelper;
using Serilog;

namespace ProcessCompare
{
    internal class DynamoImporter
    {
        private Settings _settings;
        private AmazonDynamoDBClient _client;

        public DynamoImporter(Settings settings)
        {
            _settings = settings;
            _client = new AmazonDynamoDBClient();
        }


        public void DoImport()
        {
            Log.Information("Commencing import of csv into Dynamo DB tables.");

            foreach (var file in GetFileList())
            {
                CreateTable(file);
                ImportData(file);
            }

            Log.Information("Completed import of csv into Dynamo DB tables.");
        }

        private void ImportData(FileInfo file)
        {
            throw new System.NotImplementedException();
        }

        private void CreateTable(FileInfo file)
        {
            var tableName = Path.GetFileNameWithoutExtension(file.Name);
            var request = new CreateTableRequest
            {
                TableName = tableName
            };

            var attributeDefinitions = new List<AttributeDefinition>();

            var csv = new CsvReader(File.OpenText(file.FullName));
            csv.ReadHeader();

        



        }

        private IList<FileInfo> GetFileList()
        {
            var d = new DirectoryInfo(_settings.CompareExportFolder);
            return d.GetFiles("*.csv");
        }
    }
}