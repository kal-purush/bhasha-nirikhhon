using System.Text.Json;

using Model;

namespace Handlers;

public static class Db
{
    private readonly static string DB_FILE = "data.json";
    private readonly static string filepath = Path.Join(".", "db", DB_FILE);

    private static async Task<List<Account>?> ReadAll()
    {
        var content = await File.ReadAllTextAsync(filepath);

        if (content is null || content == String.Empty)
        {
            return null;
        }

        return JsonSerializer.Deserialize<List<Account>>(content);
    }

    public static async Task Reset()
    {
        await File.WriteAllTextAsync(filepath, String.Empty);
    }

    public static async Task Create(Account account)
    {
        List<Account>? data = await ReadAll();

        if (data is null)
        {
            data = new() { account };
        }
        else
        {
            data.Add(account);
        }

        await File.WriteAllTextAsync(filepath, JsonSerializer.Serialize(data));
    }

    public static async Task<Account?> Read(string id)
    {
        var data = await ReadAll();

        if (data is null)
        {
            return null;
        }

        foreach (var element in data)
        {
            if (element.Id == id)
            {
                return element;
            }
        }

        return null;
    }

    public static async Task<Account> Increment(Account account, decimal amount)
    {
        return await Update(account, amount);
    }

    public static async Task<Account> Decrement(Account account, decimal amount)
    {
        return await Update(account, -amount);
    }

    private static async Task<Account> Update(Account account, decimal amount)
    {
        List<Account>? data = await ReadAll();

        if (data is null)
        {
            throw new Exception();
        }

        Account updatedAccount = new();
        foreach (var element in data)
        {
            if (element.Id == account.Id)
            {
                element.Balance = element.Balance + amount;
                updatedAccount.Id = element.Id;
                updatedAccount.Balance = element.Balance;
            }
        }

        await File.WriteAllTextAsync(filepath, JsonSerializer.Serialize(data));

        return updatedAccount;
    }
}