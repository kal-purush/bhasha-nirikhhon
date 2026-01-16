using System;
using System.Collections.Generic;
using System.Linq;
using Bango.SqlImport.DataReader;
using Bango.SqlImport.DataReader.Configuration;
using NUnit.Framework;

namespace Bango.SqlImport.Tests
{
    public class DataReaderFactoryTests
    {
        private DataReaderFactory factory;

        [OneTimeSetUp]
        public void SetUp()
        {
            factory = new DataReaderFactory();
        }

        [Test]
        public void GetCsvDataReader_NoCsvFilePath_ThrowsException()
        {
            var configuration = GetValidConfiguration();
            configuration.CsvFilePath = null;
            Assert.Throws<ArgumentException>(() => factory.GetCsvDataReader(configuration));
        }

        [TestCase(0)]
        [TestCase(-1)]
        public void GetCsvDataReader_InvalidColumnsCount_ThrowsException(int columnsCount)
        {
            var configuration = GetValidConfiguration();
            configuration.CsvColumnsCount = columnsCount;
            Assert.Throws<ArgumentException>(() => factory.GetCsvDataReader(configuration));
        }

        [Test]
        public void GetCsvDataReader_NoColumnDefinitions_ThrowsException()
        {
            var configuration = GetValidConfiguration();
            configuration.ColumnDefinitions = null;
            Assert.Throws<ArgumentException>(() => factory.GetCsvDataReader(configuration));
        }

        [Test]
        public void GetCsvDataReader_EmptyColumnDefinitions_ThrowsException()
        {
            var configuration = GetValidConfiguration();
            configuration.ColumnDefinitions = new List<ColumnDefinition>();
            Assert.Throws<ArgumentException>(() => factory.GetCsvDataReader(configuration));
        }

        [Test]
        public void GetCsvDataReader_DuplicateColumnNames_ThrowsException()
        {
            var configuration = GetValidConfiguration();
            configuration.ColumnDefinitions.ForEach(cd => cd.ColumnName = "SameName");
            Assert.Throws<ArgumentException>(() => factory.GetCsvDataReader(configuration));
        }

        [Test]
        public void GetCsvDataReader_DuplicateColumnIndexes_ThrowsException()
        {
            var configuration = GetValidConfiguration();
            configuration.ColumnDefinitions.Where(cd => cd.ValueSource == ValueSource.ColumnValue).ToList()
                .ForEach(cd => cd.ColumnIndex = 1);
            Assert.Throws<ArgumentException>(() => factory.GetCsvDataReader(configuration));
        }

        [Test]
        public void GetCsvDataReader_InvalidDataType_ThrowsException()
        {
            var configuration = GetValidConfiguration();
            configuration.ColumnDefinitions.ForEach(cd => cd.ColumnDataType = "InvalidType");
            Assert.Throws<ArgumentException>(() => factory.GetCsvDataReader(configuration));
        }

        [Test]
        public void GetCsvDataReader_NoValueForStaticValueColumn_ThrowsException()
        {
            var configuration = GetValidConfiguration();
            configuration.ColumnDefinitions.Where(cd => cd.ValueSource == ValueSource.StaticValue).ToList().ForEach(cd => cd.Params = null);
            Assert.Throws<ArgumentException>(() => factory.GetCsvDataReader(configuration));
        }

        [Test]
        public void GetCsvDataReader_ValidConfiguration_ReturnsReader()
        {
            var configuration = GetValidConfiguration();
            var reader = factory.GetCsvDataReader(configuration);
            Assert.IsNotNull(reader);
        }

        [Test]
        public void GetCsvDataReader_ValidConfiguration_NoStaticValues_ReturnsReader()
        {
            var configuration = GetValidConfiguration();
            configuration.ColumnDefinitions = configuration.ColumnDefinitions
                .Where(cd => cd.ValueSource != ValueSource.StaticValue).ToList();
            var reader = factory.GetCsvDataReader(configuration);
            Assert.IsNotNull(reader);
        }

        private static CsvDataReaderConfiguration GetValidConfiguration()
        {
            return new CsvDataReaderConfiguration
            {
                CsvFilePath = @"Files\ExampleFileToImport.csv",
                CsvColumnsCount = 5,
                CsvHasHeaderRow = true,
                ColumnDefinitions = new List<ColumnDefinition>
                {
                    new ColumnDefinition
                    {
                        ColumnIndex = 1,
                        ColumnName = "ColumnOne",
                        ColumnDataType = "String",
                        ValueSource = ValueSource.ColumnValue,
                        Params = new Dictionary<string, object>
                        {
                            {"key1", "value1"}
                        }
                    },
                    new ColumnDefinition
                    {
                        ColumnIndex = 2,
                        ColumnName = "ColumnTwo",
                        ColumnDataType = "Int32",
                        ValueSource = ValueSource.ColumnValue,
                        Params = new Dictionary<string, object>
                        {
                            {"key1", "value1"},
                            {"key2", "value2"}
                        }
                    },
                    new ColumnDefinition
                    {
                        ColumnName = "ColumnThree",
                        ColumnDataType = "Int32",
                        ValueSource = ValueSource.StaticValue,
                        Params = new Dictionary<string, object>
                        {
                            {"value", "value1"}
                        }
                    },
                    new ColumnDefinition
                    {
                        ColumnName = "ColumnFour",
                        ColumnDataType = "Int32",
                        ValueSource = ValueSource.StaticValue,
                        Params = new Dictionary<string, object>
                        {
                            {"value", "value1"}
                        }
                    }
                }
            };
        }
    }
}