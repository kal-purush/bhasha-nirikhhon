using Microsoft.Data.Sqlite;

namespace M0glan.Sqlite.Utils.Test
{
    public class SqliteConnectionEncryptionExtensionsTest
    {
        [Fact]
        public void SetPassword_Should_EncryptDatabase_When_DatabaseIsFresh()
        {
            string password = "Secret123";

            string? dataSource = TestUtils.GenerateTestFileName(string.Empty, "db3");
            string connectionString = new SqliteConnectionStringBuilder() { DataSource = dataSource, Mode = SqliteOpenMode.ReadWriteCreate }.ConnectionString;

            using (var connection = new SqliteConnection(connectionString))
            {
                connection.Open();
                connection.SetPassword(password);
            }

            connectionString = new SqliteConnectionStringBuilder() { DataSource = dataSource, Mode = SqliteOpenMode.ReadWrite }.ConnectionString;

            using (var connection = new SqliteConnection(connectionString))
            {
                connection.Open();
                Assert.False(connection.Test());
            }
            
            connectionString = new SqliteConnectionStringBuilder() { DataSource = dataSource, Password = "xyz123" }.ConnectionString;
            using (var connection = new SqliteConnection(connectionString))
            {
                var exception = Assert.Throws<SqliteException>(() => connection.Open());
                Assert.True(exception is SqliteException);
                Assert.Equal(SqliteErrorCodes.NOT_A_DB, exception.SqliteErrorCode);
            }

            connectionString = new SqliteConnectionStringBuilder() { DataSource = dataSource, Password = password }.ConnectionString;
            using (var connection = new SqliteConnection(connectionString))
            {
                connection.Open();
                Assert.True(connection.Test());
            }
        }

        [Fact]
        public async Task SetPasswordAsync_Should_EncryptDatabase_When_DatabaseIsFresh()
        {
            string password = "Secret123";

            string? dataSource = TestUtils.GenerateTestFileName(string.Empty, "db3");
            string connectionString = new SqliteConnectionStringBuilder() { DataSource = dataSource, Mode = SqliteOpenMode.ReadWriteCreate }.ConnectionString;

            using (var connection = new SqliteConnection(connectionString))
            {
                await connection.OpenAsync();
                await connection.SetPasswordAsync(password);
            }

            connectionString = new SqliteConnectionStringBuilder() { DataSource = dataSource, Mode = SqliteOpenMode.ReadWrite }.ConnectionString;

            using (var connection = new SqliteConnection(connectionString))
            {
                await connection.OpenAsync();
                Assert.False(await connection.TestAsync());
            }
            
            connectionString = new SqliteConnectionStringBuilder() { DataSource = dataSource, Password = "xyz123" }.ConnectionString;
            using (var connection = new SqliteConnection(connectionString))
            {
                var exception = await Assert.ThrowsAsync<SqliteException>(async () => await connection.OpenAsync());
                Assert.True(exception is SqliteException);
                Assert.Equal(SqliteErrorCodes.NOT_A_DB, exception.SqliteErrorCode);
            }

            connectionString = new SqliteConnectionStringBuilder() { DataSource = dataSource, Password = password }.ConnectionString;
            using (var connection = new SqliteConnection(connectionString))
            {
                await connection.OpenAsync();
                Assert.True(await connection.TestAsync());
            }
        }
    }
}