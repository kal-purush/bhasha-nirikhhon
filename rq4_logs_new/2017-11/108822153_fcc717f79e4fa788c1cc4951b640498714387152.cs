using System.ComponentModel;
using static DCode.Common.Enums;

namespace DCode.Common
{
    public static class EnumerationsHelper
    {
        public static string GetDescription<T>(this T value) where T : struct
        {
            var fi = value.GetType().GetField(value.ToString());

            var attributes =
                 (DescriptionAttribute[])fi.GetCustomAttributes(
                 typeof(DescriptionAttribute),
                 false);

            if (attributes != null &&
                attributes.Length > 0)
                return attributes[0].Description;
            else
                return value.ToString();
        }
    }
}