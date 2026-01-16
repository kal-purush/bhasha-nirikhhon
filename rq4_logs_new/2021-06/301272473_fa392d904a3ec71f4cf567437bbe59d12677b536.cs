using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.DataStore;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;
using ExecutionContext = CluedIn.Core.ExecutionContext;

namespace CluedIn.Connector.SqlServer.Integration.Tests
{
    public class SqlServerConnectorTests
    {
        [Fact]
        public async Task PerformanceFtw()
        {
            var configurationRepositoryMock = new Mock<IConfigurationRepository>();
            configurationRepositoryMock
                .Setup(x => x.GetConfigurationById(It.IsAny<ExecutionContext>(), It.IsAny<Guid>()))
                .Returns(new Dictionary<string, object>
                {
                    {SqlServerConstants.KeyName.Username, "sa"},
                    {SqlServerConstants.KeyName.Password, "yourStrong(!)Password"},
                    {SqlServerConstants.KeyName.Host, "localhost"},
                    {SqlServerConstants.KeyName.DatabaseName, "Streams"}
                });

            var logger = Mock.Of<ILogger<SqlServerConnector>>();
            var sqlClient = new SqlClient();

            var sqlServerConnector = new SqlServerConnector(configurationRepositoryMock.Object, logger, sqlClient);
            var executionContext = Mock.Of<ExecutionContext>();
            var providerDefinitionId = Guid.NewGuid();

            var dataTypes = new List<ConnectionDataType>
            {
                new ConnectionDataType {Name = "OriginEntityCode", Type = VocabularyKeyDataType.Text},
                new ConnectionDataType {Name = "FirstName", Type = VocabularyKeyDataType.Text},
                new ConnectionDataType {Name = "MiddleName", Type = VocabularyKeyDataType.Text},
                new ConnectionDataType {Name = "LastName", Type = VocabularyKeyDataType.Text},
                new ConnectionDataType {Name = "VeryLastName", Type = VocabularyKeyDataType.Text},
                new ConnectionDataType {Name = "VeryVeryLastName", Type = VocabularyKeyDataType.Text},
                new ConnectionDataType {Name = "MaidenName", Type = VocabularyKeyDataType.Text},
                new ConnectionDataType {Name = "IronMaidenName", Type = VocabularyKeyDataType.Text},
                new ConnectionDataType {Name = "JustName", Type = VocabularyKeyDataType.Text},
                new ConnectionDataType {Name = "Name", Type = VocabularyKeyDataType.Text},
                new ConnectionDataType {Name = "Surname", Type = VocabularyKeyDataType.Text}
            };

            //var createContainerModel = new CreateContainerModel
            //{
            //    Name = "Test",
            //    DataTypes = dataTypes
            //};

            //await sqlServerConnector.CreateContainer(executionContext, providerDefinitionId, createContainerModel);

            for (var i = 0; i < 1_000_000; i++)
            {
                //for (var j = 0; j < 10; j++) // this loop is to test duplicates ingestion
                //{
                    var data = new Dictionary<string, object>();

                    foreach (var connectionDataType in dataTypes)
                    {
                        data.Add(
                            connectionDataType.Name,
                            connectionDataType.Name == "OriginEntityCode"
                                ? $"OriginEntityCode: {i}"
                                : Guid.NewGuid().ToString());
                    }

                    await sqlServerConnector.StoreData(executionContext, providerDefinitionId, "Test", data);
                //}
            }
        }
    }
}


/*
MERGE Test AS target
USING (
	SELECT
		[OriginEntityCode],
		[FirstName],
		[MiddleName],
		[LastName],
		[VeryLastName],
		[VeryVeryLastName],
		[MaidenName],
		[IronMaidenName],
		[JustName],
		[Name],
		[Surname]
	FROM
		Temp) AS source
	ON target.[OriginEntityCode] = source.[OriginEntityCode]
	WHEN MATCHED THEN
	UPDATE SET
		target.[OriginEntityCode] = source.[OriginEntityCode],
		target.[FirstName] = source.[FirstName],
		target.[MiddleName] = source.[MiddleName],
		target.[LastName] = source.[LastName],
		target.[VeryLastName] = source.[VeryLastName],
		target.[VeryVeryLastName] = source.[VeryVeryLastName],
		target.[MaidenName] = source.[MaidenName],
		target.[IronMaidenName] = source.[IronMaidenName],
		target.[JustName] = source.[JustName],
		target.[Name] = source.[Name],
		target.[Surname] = source.[Surname]		
	WHEN NOT MATCHED THEN
	INSERT (
		[OriginEntityCode],
		[FirstName],
		[MiddleName],
		[LastName],
		[VeryLastName],
		[VeryVeryLastName],
		[MaidenName],
		[IronMaidenName],
		[JustName],
		[Name],
		[Surname])
	VALUES (
		source.[OriginEntityCode],
		source.[FirstName],
		source.[MiddleName],
		source.[LastName],
		source.[VeryLastName],
		source.[VeryVeryLastName],
		source.[MaidenName],
		source.[IronMaidenName],
		source.[JustName],
		source.[Name],
		source.[Surname]);

 */