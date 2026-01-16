using System.Globalization;

namespace ServiPuntos.uy_mobile.Converters
{
  public class BoolToAgeRestrictedConverter : IValueConverter
  {
    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
      if (value is bool isRestricted)
        return isRestricted ? "+18" : "todas las edades";

      return "ni idea";
    }

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
      throw new NotImplementedException();
    }
  }
}