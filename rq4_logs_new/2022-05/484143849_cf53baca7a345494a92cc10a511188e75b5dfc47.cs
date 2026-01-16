namespace Prode2022Server.Helpers
{
    public static class Extensions
    {
        public static bool IsNullOrEmpty(this string? str)
        {
            return string.IsNullOrEmpty(str);
        }

        public static bool IsNullOrZero(this int? value)
        {
            return value == null || value == 0;
        }
    }
}