namespace M0glan.Sqlite.Utils;

using Microsoft.Data.Sqlite;

public class SqlCipherEncryption : ISqliteEncryption
{
    private const string READ_SQL = "SELECT COUNT(*) FROM sqlite_master";

    public void SetPassword(string dataSource, string currentPassword = "", string newPassword = "")
    {
        if (string.IsNullOrWhiteSpace(dataSource))
        {
            throw new ArgumentNullException(nameof(dataSource));
        }

        if (!File.Exists(dataSource))
        {
            CreateWithPassword(dataSource, newPassword);
        }

        using var connection = new SqliteConnection(BuildConnectionString(dataSource, currentPassword, SqliteOpenMode.ReadWriteCreate));
        connection.Open();
        connection.SetPassword(newPassword);
    }

    private void CreateWithPassword(string dataSource, string password)
    {
        using var connection = new SqliteConnection(BuildConnectionString(dataSource, password, SqliteOpenMode.ReadWriteCreate));
        connection.Open();
    }

    private void ChangePassword(string dataSource, string currentPassword, string newPassword)
    {

    }

    public async Task SetPasswordAsync(string dataSource, string currentPassword = "", string newPassword = "", CancellationToken cancellationToken = default)
    {
        await Task.FromException(new NotImplementedException());
    }

    public bool TestPassword(string dataSource, string password = "")
    {
        try
        {
            Read(BuildConnectionString(dataSource, password));
            return true;
        }
        catch (SqliteException e)
        {
            if (e.ErrorCode == SqliteErrorCodes.NOT_A_DB)
            {
                return false;
            }

            throw;
        }
    }

    private void Read(string connectionString)
    {
        using var connection = new SqliteConnection(BuildConnectionString(connectionString));
        connection.Open();

        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = READ_SQL;
        command.ExecuteScalar();
    }

    public async Task<bool> TestPasswordAsync(string dataSource, string password = "", CancellationToken cancellationToken = default)
    {
        try
        {
            await ReadAsync(BuildConnectionString(dataSource, password));
            return true;
        }
        catch (SqliteException e)
        {
            if (e.ErrorCode == SqliteErrorCodes.NOT_A_DB)
            {
                return false;
            }

            throw;
        }
    }

    private static async Task ReadAsync(string connectionString)
    {
        using var connection = new SqliteConnection(BuildConnectionString(connectionString));
        await connection.OpenAsync();

        using SqliteCommand command = connection.CreateCommand();
        command.CommandText = READ_SQL;
        await command.ExecuteScalarAsync();
    }

    private static string BuildConnectionString(string dataSource, string password = "", SqliteOpenMode mode = SqliteOpenMode.ReadOnly)
    {
        var builder = new SqliteConnectionStringBuilder
        {
            DataSource = dataSource,
            Password = password,
            Mode = mode
        };
        return builder.ConnectionString;
    }
}