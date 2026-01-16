namespace UniversityOrderAPI.BLL;

public static class StringExtension
{
    public static bool IsDigitsOnly(this string str) => str.All(c => c is >= '0' and <= '9');
}