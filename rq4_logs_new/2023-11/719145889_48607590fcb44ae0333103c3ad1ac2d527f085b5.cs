using Dapper;
using Npgsql;
using System.Data;

namespace legallead.jdbc.helpers
{
    public class DataContext
    {
        private readonly string _connectionString;
        public DataContext()
        {
            _connectionString = RemoteData.GetPostGreString();
        }
        public virtual IDbConnection CreateConnection()
        {
            return new NpgsqlConnection(_connectionString);
        }
        public async Task Init()
        {
            await InitTables();
            await InitApplications();
            await InitProfile();
        }
        private async Task InitTables()
        {
            // create tables if they don't exist
            using var connection = CreateConnection();
            var sql = "CREATE TABLE IF NOT EXISTS APPLICATIONS ( "
                + Environment.NewLine
                + "\tId CHAR(36) PRIMARY KEY, "
                + Environment.NewLine
                + "\tName VARCHAR(150) "
                + Environment.NewLine
                + ");"
                + Environment.NewLine
                + ""
                + Environment.NewLine
                + "CREATE TABLE IF NOT EXISTS USERS ("
                + Environment.NewLine
                + "\tId CHAR(36) PRIMARY KEY,"
                + Environment.NewLine
                + "\tUserName VARCHAR(50),"
                + Environment.NewLine
                + "\tEmail VARCHAR(255),"
                + Environment.NewLine
                + "\tPasswordHash VARCHAR(150),"
                + Environment.NewLine
                + "\tPasswordSalt VARCHAR(150)"
                + Environment.NewLine
                + ");"
                + Environment.NewLine
                + ""
                + Environment.NewLine
                + "CREATE TABLE IF NOT EXISTS PROFILEMAP ("
                + Environment.NewLine
                + "\tId CHAR(36) PRIMARY KEY,"
                + Environment.NewLine
                + "\tOrderId INT,"
                + Environment.NewLine
                + "\tKeyName VARCHAR(100)"
                + Environment.NewLine
                + ");"
                + Environment.NewLine
                + ""
                + Environment.NewLine
                + "CREATE TABLE IF NOT EXISTS PERMISSIONMAP ("
                + Environment.NewLine
                + "\tId CHAR(36) PRIMARY KEY,"
                + Environment.NewLine
                + "\tOrderId INT,"
                + Environment.NewLine
                + "\tKeyName VARCHAR(100)"
                + Environment.NewLine
                + ");"
                + Environment.NewLine
                + ""
                + Environment.NewLine
                + "CREATE TABLE IF NOT EXISTS USERPROFILE ("
                + Environment.NewLine
                + "\tId CHAR(36) PRIMARY KEY,"
                + Environment.NewLine
                + "\tUserId CHAR(36) references USERS(Id) ,"
                + Environment.NewLine
                + "\tProfileMapId CHAR(36) references PROFILEMAP(Id) ,"
                + Environment.NewLine
                + "\tKeyValue VARCHAR(256)"
                + Environment.NewLine
                + ");"
                + Environment.NewLine
                + ""
                + Environment.NewLine
                + "CREATE TABLE IF NOT EXISTS USERPERMISSION ("
                + Environment.NewLine
                + "\tId CHAR(36) PRIMARY KEY,"
                + Environment.NewLine
                + "\tUserId CHAR(36) references USERS(Id),"
                + Environment.NewLine
                + "\tPermissionMapId CHAR(36) references PERMISSIONMAP(Id) ,"
                + Environment.NewLine
                + "\tKeyValue VARCHAR(256)"
                + Environment.NewLine
                + ");";
            var stmts = sql.Split(';', StringSplitOptions.RemoveEmptyEntries);
            foreach (var stmt in stmts)
            {
                if (!string.IsNullOrEmpty(stmt))
                {
                    var command = $"{stmt};";
                    await connection.ExecuteAsync(command);
                }
            }

        }

        private async Task InitApplications()
        {
            var applicationNames = "legallead.permissions.api".Split(',');
            var command = "INSERT INTO APPLICATIONS " + Environment.NewLine +
            "( Id, Name ) " + Environment.NewLine +
            "SELECT b.Id, b.Name " + Environment.NewLine +
            "FROM APPLICATIONS as a " + Environment.NewLine +
            "RIGHT JOIN " + Environment.NewLine +
            "( " + Environment.NewLine +
            "SELECT  " + Environment.NewLine +
            "\t'{0}' Id, " + Environment.NewLine +
            "\t'{1}' Name " + Environment.NewLine +
            ") as b " + Environment.NewLine +
            "ON a.Name = b.Name " + Environment.NewLine +
            "WHERE a.Name IS NULL;";

            using var connection = CreateConnection();
            foreach (var application in applicationNames)
            {
                var indx = Guid.NewGuid().ToString("D").ToLower();
                var stmt = string.Format(command, indx, application);
                await connection.ExecuteAsync(stmt);
            }
        }


        private async Task InitProfile()
        {
            var keynames = new List<string> {
            "First Name",
            "Last Name",
            "Company Name",
            "Phone 1",
            "Phone 2",
            "Phone 3",
            "Email 1",
            "Email 2",
            "Email 3",
            "Address 1 - Address Line 1",
            "Address 1 - Address Line 2",
            "Address 1 - Address Line 3",
            "Address 1 - Address Line 4",
            "Address 2 - Address Line 1",
            "Address 2 - Address Line 2",
            "Address 2 - Address Line 3",
            "Address 2 - Address Line 4",
        };
            var command = "INSERT INTO PROFILEMAP " + Environment.NewLine +
            "( Id, OrderId, KeyName ) " + Environment.NewLine +
            "SELECT b.Id, b.OrderId, b.KeyName " + Environment.NewLine +
            "FROM PROFILEMAP as a " + Environment.NewLine +
            "RIGHT JOIN " + Environment.NewLine +
            "( " + Environment.NewLine +
            "SELECT  " + Environment.NewLine +
            "\t'{0}' Id, " + Environment.NewLine +
            "\t {1}  OrderId, " + Environment.NewLine +
            "\t'{2}' KeyName " + Environment.NewLine +
            ") as b " + Environment.NewLine +
            "ON a.KeyName = b.KeyName " + Environment.NewLine +
            "WHERE a.KeyName IS NULL;";

            using var connection = CreateConnection();
            foreach (var key in keynames)
            {
                var indx = Guid.NewGuid().ToString("D").ToLower();
                var stmt = string.Format(command, indx, keynames.IndexOf(key), key);
                await connection.ExecuteAsync(stmt);
            }
        }
    }
}