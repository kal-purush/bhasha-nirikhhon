//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

using System;
using Cassandra;
using Cassandra.Mapping;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;
using System.Threading;
using System.Dynamic;

namespace CassandraChangeFeedSample
{
    public class Program
    {
        // Cassandra Cluster Configs      
        private const string UserName = "UserName";
        private const string Password = "Password";
        private const string CassandraContactPoint = "CassandraContactPoint";  // DnsName  
        private static int CassandraPort = 10350;
        private static ISession session;

        public static void Main(string[] args)
        {
            Program p = new Program();
            p.ChangeFeedPull();
        }

        public void ChangeFeedPull()
        {
            // Connect to cassandra cluster  (Cassandra API on Azure Cosmos DB supports only TLSv1.2)
            var options = new Cassandra.SSLOptions(SslProtocols.Tls12, true, ValidateServerCertificate);
            options.SetHostNameResolver((ipAddress) => CassandraContactPoint);
            Cluster cluster = Cluster.Builder().WithCredentials(UserName, Password).WithPort(CassandraPort).AddContactPoint(CassandraContactPoint).WithSSL(options).Build();
            
            session = cluster.Connect();
            Setup(); //drop and create keyspace and table
            session = cluster.Connect("uprofile");
            IMapper mapper = new Mapper(session);

            Console.WriteLine("pulling from change feed: ");

            //set initial start time for pulling the change feed
            DateTime timeBegin = DateTime.UtcNow;

            //initialise variable to store the continuation token
            byte[] pageState = null;
            while (true)
            {
                try
                {
                    
                    IStatement changeFeedQueryStatement = new SimpleStatement(
                    $"SELECT * FROM uprofile.user where COSMOS_CHANGEFEED_START_TIME() = '{timeBegin.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture)}'");
                    if (pageState != null)
                    {
                        changeFeedQueryStatement = changeFeedQueryStatement.SetPagingState(pageState);
                    }
                    Console.WriteLine("getting records from change feed at last page state....");
                    RowSet rowSet = session.Execute(changeFeedQueryStatement);
                    
                    //store the continuation token here
                    pageState = rowSet.PagingState;
                   
                    List<Row> rowList = rowSet.ToList();
                    if (rowList.Count != 0)
                    {
                        for (int i = 0; i < rowList.Count; i++)
                        {
                            string value = rowList[i].GetValue<string>("user_name");
                            int key = rowList[i].GetValue<int>("user_id");
                            // do something with the data - e.g. compute, forward to another event, function, etc.
                            // here, we just print the user name field
                            Console.WriteLine("user_name: " + value);
                        }
                    }
                    else
                    {
                        Console.WriteLine("zero documents read");
                    }
                    Thread.Sleep(300);

                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception " + e);
                }
            }
        }

        public static bool ValidateServerCertificate(
            object sender,
            X509Certificate certificate,
            X509Chain chain,
            SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;

            Console.WriteLine("Certificate error: {0}", sslPolicyErrors);
            // Do not allow this client to communicate with unauthenticated servers.
            return false;
        }

        public void Setup()
        {
            //Creating KeySpace and table
            session.Execute("DROP KEYSPACE IF EXISTS uprofile");
            session.Execute("CREATE KEYSPACE uprofile WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 };");
            Console.WriteLine(String.Format("created keyspace uprofile"));
            session.Execute("CREATE TABLE IF NOT EXISTS uprofile.user (user_id int PRIMARY KEY, user_name text, user_bcity text)");
            Console.WriteLine(String.Format("created table user"));
            //sleep to ensure Keyspace and table written before querying starts
            Thread.Sleep(2000);
        }      
    }
}