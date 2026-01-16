using System.Reflection.Metadata;

using Handlers;

namespace Models;

public static class DBHandler
{
    public static async Task<Partner?> GetPartnerData(string partnerId)
    {
        try
        {
            string filepath = Path.Join(".", "partnerData.json");
            Partner? partner = await JsonHandler.ReadAll<Partner>(filepath);

            return partner;
        }
        catch (Exception error)
        {
            Console.WriteLine(error.Message);

        }

        return null;
    }
}