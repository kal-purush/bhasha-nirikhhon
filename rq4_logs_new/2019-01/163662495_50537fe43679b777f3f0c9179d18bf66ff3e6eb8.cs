using System;
using System.Net.Sockets;
using Amazon;
using Amazon.DynamoDBv2;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using System.Reflection;

namespace AWSDynamoDB
{
    public class HashKeyAttr:Attribute
    {
    }

    public  class DynamoDB
    {
        
        static bool connected = false;
        static AmazonDynamoDBClient _client = null;
        static string accessKey = "AKIAJK4VO4LM4K2N7STA";
        static string secretKey = "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";
        static string pwd = "H80'[R}TdRMG";
        static string region = "us-east-1";
        static TableDescription moviesTableDescription;
        static ProvisionedThroughput provisionedThroughput;

        public DynamoDB(bool useDynamoDBLocal)
        {
            if (_client != null)
                return;
            if (useDynamoDBLocal)
            {

                Console.WriteLine("  -- Setting up a DynamoDB-Local client (DynamoDB Local seems to be running)");
                AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
                ddbConfig.ServiceURL = "http://localhost:8000";
                
                try
                {
                    _client = new AmazonDynamoDBClient(ddbConfig);
                }
                catch (Exception ex)
                {
                    throw new Exception("FAILED to create a DynamoDBLocal client; " + ex.Message);

                }
            }

            else
            {
                try
                {
                    var credentials = new BasicAWSCredentials(accessKey, secretKey);
                    _client = new AmazonDynamoDBClient(credentials, RegionEndpoint.USEast1);                     
                }
                catch (Exception ex)
                {
                    Console.WriteLine("     FAILED to create a DynamoDB client; " + ex.Message);
                    
                }
            }
            connected = true;
        provisionedThroughput = new ProvisionedThroughput
        {
            ReadCapacityUnits = 1000,
            WriteCapacityUnits = 1000

        };
        Console.WriteLine("connected successfully");
            

        }
        /***********************************************/
        static AttributeDefinition GetAttributeDefinition(PropertyInfo p)
        {
            Type type = p.PropertyType;
            return new AttributeDefinition
            {
                AttributeName = p.Name,
                AttributeType = type ==typeof(string)? "S":type==typeof(bool)?"B":"N"

            };
        }
        /**********************************************/
        static KeySchemaElement GetSchemaElement(PropertyInfo p)
        {
            Attribute attr = p.GetCustomAttribute(typeof(HashKeyAttr),true);
            return new KeySchemaElement
            {
                AttributeName = p.Name,
                KeyType = attr != null ? KeyType.HASH : KeyType.RANGE
                
            };
        }
        /***********************************************/
        public async Task<bool>  ToTable(Type type)
        {
            DynamoDB db = new DynamoDB(true);
            List<AttributeDefinition> attrDets = new List<AttributeDefinition>();
            List<KeySchemaElement> keys = new List<KeySchemaElement>();
            foreach (PropertyInfo p in type.GetProperties())
            {
                attrDets.Add(GetAttributeDefinition(p));
                keys.Add(GetSchemaElement(p));
                
            }
            
            
            CreateTableRequest request = new CreateTableRequest
            {
                TableName = type.Name,
                AttributeDefinitions = attrDets,
                KeySchema = keys,
                ProvisionedThroughput = provisionedThroughput
            };
            bool result = default(bool);
            try
            {
                result = await db.CreatTable(request);// type.Name, attrDets, keys, provisionedThroughput);
                
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex + "::" + ex.Message);
                Exception e = ex.InnerException;

                while (e != null)
                {
                    Console.WriteLine("Inner::" + e.Message);
                    e = e.InnerException;
                }
                throw e;
            }
            return true;
        }
            /**********************************************/
            public  async Task<bool> CreatTable(CreateTableRequest request)
        {
            if (!connected)
                return false;
            
            try
            {
                bool exists = await TableExists(request.TableName);
                if (exists)
                {
                    return true;
                }
            }

            catch (Exception e)
            {
                 Console.WriteLine(e + e.Message);
                throw new OperationCanceledException(e.Message + ":cant check if table exist");

            }

            CreateTableResponse response;

            try
            {
                Console.WriteLine("  -- Creat Table {0}...", request.TableName);
                response = await _client.CreateTableAsync(request);

                Console.WriteLine("     -- Created the \"{0}\" table successfully!", request.TableName);

            }
            catch (Exception ex)
            {
                Console.WriteLine(" FAILED to create the new table, because: {0}.", ex.Message);
                throw ex;
            }

            // Report the status of the new table...
            Console.WriteLine("     Status of the new table: '{0}'.", response.TableDescription.TableStatus);
            moviesTableDescription = response.TableDescription;
            return true;
                        
        }


        /*******************************************/
         async Task<bool> TableExists(string tableName)
        {
            DescribeTableResponse descResponse;

            try
            {
                ListTablesResponse tblResponse = await _client.ListTablesAsync();
                
                if (tblResponse.TableNames.Contains(tableName))
                {
                    Console.WriteLine("     A table named {0} already exists in DynamoDB!", tableName);

                    // If the table exists, get its description
                    try
                    {
                        descResponse = await _client.DescribeTableAsync(tableName);

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("     However, its description is not available ({0})", ex.Message);
                        moviesTableDescription = null;
                        throw ex;

                    }
                    moviesTableDescription = descResponse.Table;
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("faild" + ex); throw ex;
            }

            return false;
        }


        /**********************************************/
        
    }
}
