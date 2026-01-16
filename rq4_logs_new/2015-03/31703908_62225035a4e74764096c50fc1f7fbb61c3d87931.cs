using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SQLObjectBackup;
using System.Linq;
using System.Configuration;

namespace SQLObjectBackup.UnitTesting
{
    [TestClass]
    public class DatabaseTests
    {
        private string _sqlServerName;
        private string _databaseName;
        private string _testObjectSchemaName;
        private string _testObjectName;
        private SqlDatabase _sqlDatabase;

        public DatabaseTests()
        {
            _sqlServerName = ConfigurationManager.AppSettings["SqlServerName"];
            _databaseName = ConfigurationManager.AppSettings["DatabaseName"];
            _testObjectSchemaName = ConfigurationManager.AppSettings["TestObjectSchemaName"];
            _testObjectName = ConfigurationManager.AppSettings["TestObjectName"];

            _sqlDatabase = new SqlDatabase(_sqlServerName, _databaseName);
        }

        [TestMethod]
        public void SuccessConnection()
        {
            SqlDatabase sqlDatabase = new SqlDatabase(_sqlServerName, _databaseName);
            Assert.IsTrue(sqlDatabase.TestConnection());
        }

        [TestMethod]
        public void TablesCount()
        {
            Assert.IsTrue(_sqlDatabase.Tables.Count() > 0);
        }

        [TestMethod]
        public void TableName()
        {
            Assert.IsNotNull(_sqlDatabase.Tables.Where(m => m.SchemaName == _testObjectSchemaName && m.ObjectName == _testObjectName));
        }

        [TestMethod]
        public void QuotedTableName()
        {
            SqlTable sqlTable = _sqlDatabase.GetTable(_testObjectSchemaName, _testObjectName);
            Assert.AreEqual(string.Format("[{0}].[{1}]", _testObjectSchemaName, _testObjectName), sqlTable.FullyQuotedName);
        }

        [TestMethod]
        public void RowCount()
        {
            Assert.IsTrue(_sqlDatabase.GetRowCount(_sqlDatabase.GetTable(_testObjectSchemaName, _testObjectName)) > 0);
        }

        [TestMethod]
        public void BaseEntities()
        {
            if (!_sqlDatabase.BaseEntitiesExist())
                _sqlDatabase.CreateBaseEntities();

            Assert.IsTrue(_sqlDatabase.BaseEntitiesExist());

            _sqlDatabase.DeleteBaseEntities();

            Assert.IsFalse(_sqlDatabase.BaseEntitiesExist());
        }

        [TestMethod]
        public void BackupTable()
        {
            if (!_sqlDatabase.BaseEntitiesExist())
                _sqlDatabase.CreateBaseEntities();

            SqlTable sourceTable = _sqlDatabase.GetTable(_testObjectSchemaName, _testObjectName);
            Assert.IsNotNull(sourceTable);

            int tableCountBeforeBackup = _sqlDatabase.GetTables().Count();

            SqlTable backupTable = _sqlDatabase.BackupTable(sourceTable);

            Assert.IsNotNull(backupTable);

            int tableCountAfterBackup = _sqlDatabase.GetTables().Count();

            Assert.IsTrue(tableCountAfterBackup == tableCountBeforeBackup + 1);

            Assert.IsNotNull(_sqlDatabase.GetTableBackup(sourceTable));
            Assert.IsTrue(_sqlDatabase.GetTableBackup(sourceTable).Count() == 1);

            Assert.AreEqual(_sqlDatabase.GetRowCount(sourceTable), _sqlDatabase.GetRowCount(backupTable));

            _sqlDatabase.RemoveBackup(sourceTable);

            int tableCountAfterBackupRemoval = _sqlDatabase.GetTables().Count();

            Assert.AreEqual(tableCountBeforeBackup, tableCountAfterBackupRemoval);
            Assert.IsTrue(_sqlDatabase.GetTableBackup(sourceTable).Count() == 0);

            if (_sqlDatabase.BaseEntitiesExist())
                _sqlDatabase.DeleteBaseEntities();
        }

        [TestMethod]
        public void RemoveAllBackups()
        {
            if (!_sqlDatabase.BaseEntitiesExist())
                _sqlDatabase.CreateBaseEntities();

            int tableCountBeforeBackups = _sqlDatabase.GetTables().Count();
            int backupCount = 5;

            int i = 0;
            foreach (SqlTable sqlTable in _sqlDatabase.GetTables())
            {
                if (i == backupCount)
                    break;

                _sqlDatabase.BackupTable(sqlTable);

                i++;
            }

            int tableCountAfterBackups = _sqlDatabase.GetTables().Count();

            Assert.IsTrue(tableCountAfterBackups == tableCountBeforeBackups + backupCount);

            _sqlDatabase.RemoveAllBackups();

            int tableCountAfterBackupsRemoved = _sqlDatabase.GetTables().Count();

            Assert.AreEqual(tableCountBeforeBackups, tableCountAfterBackupsRemoved);

            if (_sqlDatabase.BaseEntitiesExist())
                _sqlDatabase.DeleteBaseEntities();
        }

        [TestMethod]
        public void MultipleBackups()
        {
            int backupsToCreate = 4;

            if (!_sqlDatabase.BaseEntitiesExist())
                _sqlDatabase.CreateBaseEntities();

            SqlTable sourceTable = _sqlDatabase.GetTables().First();

            int tableCountBeforeBackups = _sqlDatabase.GetTables().Count();

            int i;
            for (i = 0; i < backupsToCreate; i++)
                _sqlDatabase.BackupTable(sourceTable);

            int tableCountAfterBackups = _sqlDatabase.GetTables().Count();

            Assert.IsTrue(tableCountAfterBackups == tableCountBeforeBackups + backupsToCreate);

            int backupCount = _sqlDatabase.GetTableBackup(sourceTable).Count();
            Assert.AreEqual(backupsToCreate, backupCount);

            _sqlDatabase.RemoveBackup(sourceTable);

            int tableCountAfterBackupRemoval = _sqlDatabase.GetTables().Count();

            Assert.AreEqual(tableCountBeforeBackups, tableCountAfterBackupRemoval);

            Assert.IsTrue(_sqlDatabase.GetTableBackup(sourceTable).Count() == 0);

            if (_sqlDatabase.BaseEntitiesExist())
                _sqlDatabase.DeleteBaseEntities();
        }
    }
}