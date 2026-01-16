namespace Staris.Console.Integration.Helpers;

public static class Util
{
    public static int TryParseInt(string data)
    {
        int aux;
        int.TryParse(data, out aux);
        return aux;
    }

    public static long TryParseLong(string data)
    {
        long aux;
        long.TryParse(data, out aux);
        return aux;
    }

    public static decimal TryParseDecimal(string data)
    {
        decimal aux;
        decimal.TryParse(data, out aux);
        return aux;
    }

    public static int ParseId(string data) =>
        TryParseInt(data.Split('/')[data.Split('/').Length - 2].ToString());
    
    public static DateTime TryParseDateTime(string data){
        DateTime aux;
        DateTime.TryParse(data, out aux);
        return aux;
    }
}