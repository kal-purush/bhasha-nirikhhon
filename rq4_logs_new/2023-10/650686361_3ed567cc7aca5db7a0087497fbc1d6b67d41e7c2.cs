using ShareInvest.Entities.Google;

namespace ShareInvest.Utilities.Google;

public static class Marker
{
    public static object MakeStockMarker(string? url, CoordinateStock stock)
    {
        return new
        {
            position = new
            {
                lat = stock.Latitude,
                lng = stock.Longitude
            },
            png = GetMarkerImageUrl(url, stock.CompareToPreviousSign switch
            {
                "1" or "2" => "red",
                "4" or "5" => "blue",
                _ => "black"
            }),
            code = stock.Code,
            name = stock.Name
        };
    }
    public static string GetMarkerImageUrl(string? url, string color)
    {
        return Path.Combine(url ?? "http://share.enterprises", "images", "pins", $"pin_{color}.png");
    }
}