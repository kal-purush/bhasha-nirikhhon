using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Entities;

namespace Aws.Database.DynamoDb
{
    public class DynamoDbArchiver : IArchiver<BfmEvent>
    {
        private readonly AmazonDynamoDBClient _client;
        private readonly string _tableName;
        private readonly List<BfmEvent> _hack;

        public DynamoDbArchiver()
        {
            _client = GetClient();
            _tableName = "ExtensionSampleArchive";
            _hack = new List<BfmEvent>();
        }

        private AmazonDynamoDBClient GetClient()
        {
            Debug.Write("Loading DynamoDb Client...");
            AWSConfigs.RegionEndpoint = RegionEndpoint.USEast2;
            var chain = new CredentialProfileStoreChain();
            return !chain.TryGetAWSCredentials("log_profile", out AWSCredentials credentials)
                ? null
                : new AmazonDynamoDBClient(credentials);
        }

        public void Archive(BfmEvent obj)
        {
            _hack.Add(obj);
            if (_client == null) return;

            var task = ArchiveInternal(obj);
            Debug.Write($"Archiving task running. Task id: {task.Id}");
        }

        private async Task ArchiveInternal(BfmEvent obj)
        {
            var request = new PutItemRequest
            {
                TableName = _tableName,
                Item = new Dictionary<string, AttributeValue>
                {
                    {"DocumentId", new AttributeValue {S = Guid.NewGuid().ToString()}},
                    {"EventId", new AttributeValue {S = obj.Id}},
                    {"EventSubId", new AttributeValue {S = obj.SubId}}
                }
            };

            var response = await _client.PutItemAsync(request);
            Debug.Write(
                $"Event {obj} was archive to DynamoDb. " +
                $"Status code: {response.HttpStatusCode}");
        }

        public IEnumerable<BfmEvent> Select(Func<BfmEvent, bool> pred = null)
        {
            return pred != null ? _hack.Where(pred) : _hack;
        }
    }
}