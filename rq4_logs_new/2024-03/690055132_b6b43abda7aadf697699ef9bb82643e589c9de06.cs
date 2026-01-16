namespace AirQuality.SensorService.Helpers;

public static class Helper
{
    /// <summary>
    /// Выборка широты и долготы из строки NMEA
    /// </summary>
    public static string GetLongitudeAndLatitudeStringFromLocation(string location)
    {
        if (string.IsNullOrEmpty(location) || location[0] != '$')
        {
            return "Invalid";
        }
        
        var fields = location.Split(',');
        
        if (fields.Length < 6)
        {
            return "Invalid";
        }
        
        var messageId = fields[0];
        
        if (messageId != "$GPGGA")
        {
            return "Invalid";
        }
        
        var latitude = fields[2];
        var latitudeHemisphere = fields[3];
        var longitude = fields[4];
        var longitudeHemisphere = fields[5];
        
        if (string.IsNullOrEmpty(latitude) || string.IsNullOrEmpty(latitudeHemisphere) ||
            string.IsNullOrEmpty(longitude) || string.IsNullOrEmpty(longitudeHemisphere))
        {
            return "Invalid";
        }
        
        var latitudeDecimal = Helper.ConvertToDecimalDegrees(latitude, latitudeHemisphere);
        var longitudeDecimal = Helper.ConvertToDecimalDegrees(longitude, longitudeHemisphere);
        
        var latitudeString = latitudeDecimal.ToString("F4");
        var longitudeString = longitudeDecimal.ToString("F4");
        
        var coordinates = latitudeString + "," + longitudeString;
        
        return coordinates;
    }
    
    private static double ConvertToDecimalDegrees(string expression, string hemisphere)
    {
        var degrees = Math.Floor(double.Parse(expression) / 100);
        var minutes = double.Parse(expression) % 100; 
        
        var decimalPart = minutes / 60;
        
        var decimalDegree = degrees + decimalPart;
        
        if (hemisphere is "S" or "W")
        {
            decimalDegree = -decimalDegree;
        }
        
        return decimalDegree;
    }
}