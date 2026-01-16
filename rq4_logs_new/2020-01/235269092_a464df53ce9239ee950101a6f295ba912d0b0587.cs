using System;
using System.Globalization;
using DotNetCafe.Globalization;

namespace DotNetCafe.Internals
{
    internal static class CpfFormatter
    {
        private const string GENERAL_FORMAT = "G";
        private const string GENERAL_FORMAT_VALUE = @"000\.000\.000\-00";
        private const string NUMERIC_FORMAT = "N";
        private const string NUMERIC_FORMAT_VALUE = "D11";
        private const string BAR_FORMAT = "B";
        private const string BAR_FORMAT_VALUE = @"000000000\/00";

        public static string Format(Cpf self, string? format, IFormatProvider? provider)
        {
            format ??= GENERAL_FORMAT;
            provider ??= CultureInfo.InvariantCulture;

            return format.ToUpperInvariant() switch
            {
                GENERAL_FORMAT => 
                    self.number.ToString(GENERAL_FORMAT_VALUE, provider),
                NUMERIC_FORMAT =>
                    self.number.ToString(NUMERIC_FORMAT_VALUE, provider),
                BAR_FORMAT =>
                    self.number.ToString(BAR_FORMAT_VALUE, provider),
                _ => 
                    throw new FormatException(string.Format(SR.FormatException_InvalidFormat, format))
            };
        }
    }
}