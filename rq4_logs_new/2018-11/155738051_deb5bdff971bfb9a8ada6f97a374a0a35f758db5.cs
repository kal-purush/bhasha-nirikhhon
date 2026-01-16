namespace VibeDrinkotechSever
{
    public static class ExtensionMethods
    {
        public static string FormatErrorForLog(this string value)
        {
            if (value.Length > 0)
            {
                return value.Replace("\n", "-").Replace("\r", "-");
            }
            return value;
        } 
    }
}