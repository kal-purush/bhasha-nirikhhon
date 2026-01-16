using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Net.Http.Headers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace NewNewTry.Controllers
{
    public class DynamoDbController : Controller
    {
        private const string tableName = "MVCFlowerShopTP051436";

        private List<string> getAWSCredentialInfo()
        {
            //1 set up the appsetting.json file path 
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");

            IConfiguration configuration = builder.Build();

            //2 read the key info from the json using configure instance 
            var keyList = new List<string>();
            keyList.Add(configuration["AWSCredential:key1"]);// access key
            keyList.Add(configuration["AWSCredential:key2"]);// session key
            keyList.Add(configuration["AWSCredential:key3"]);// token key

            return keyList;
        }

        // GET
        public IActionResult Index(string msg = "")
        {
            ViewBag.msg = msg;
            return View();
        }


        public async Task<IActionResult> createTable()
        {
            string msg = "";
            List<string> keylists = getAWSCredentialInfo();
            var dynamoDbClient = new AmazonDynamoDBClient(keylists[0], keylists[1], keylists[2]);
            try
            {
                var createTableRequest = new CreateTableRequest()
                {
                    TableName = tableName,
                    AttributeDefinitions = new List<AttributeDefinition>()
                    {
                        new AttributeDefinition()
                        {
                            AttributeName = "CustomerID",
                            AttributeType = "S"
                        },
                        new AttributeDefinition()
                        {
                            AttributeName = "TransactionID",
                            AttributeType = "S"
                        }
                    },
                    KeySchema = new List<KeySchemaElement>()
                    {
                        new KeySchemaElement()
                        {
                            AttributeName = "CustomerID",
                            KeyType = "HASH"
                        },
                        new KeySchemaElement()
                        {
                            AttributeName = "TransactionID",
                            KeyType = "RANGE"
                        }
                    },
                    ProvisionedThroughput = new ProvisionedThroughput()
                    {
                        ReadCapacityUnits = 2,
                        WriteCapacityUnits = 1
                    }
                };

                await dynamoDbClient.CreateTableAsync(createTableRequest);

                msg = "Table : " + tableName + " created successfully";
            }
            catch (AmazonDynamoDBException ex)
            {
                return BadRequest("Error in creating table of" + tableName + " : " + ex.Message);
            }
            catch (Exception ex)
            {
                return BadRequest("Error in creating table of" + tableName + " : " + ex.Message);
            }

            return RedirectToAction("Index", "DynamoDb", new { msg = msg });
        }
    }
}