using ShareInvest.Entities.Kiwoom;

using System.Reflection;

namespace ShareInvest;

public static class Cache
{
    public static void SaveTemporarily(string sScrNo, TR constructor)
    {
        stores[sScrNo] = constructor;
    }
    public static TR? GetConstructor(string sTrCode, string sScrNo)
    {
        if (stores.Remove(sScrNo, out TR? constructor))
        {
            return constructor;
        }
        var typeName = string.Concat(typeof(TR).Namespace, '.', sTrCode);

        return Assembly.GetExecutingAssembly().CreateInstance(typeName, true) as TR;
    }
    static readonly Dictionary<string, TR> stores = new();
}