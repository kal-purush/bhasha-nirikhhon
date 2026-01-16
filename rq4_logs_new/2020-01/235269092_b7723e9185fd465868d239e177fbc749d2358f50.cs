using System;
using System.Diagnostics;
using DotNetCafe.Globalization;

namespace DotNetCafe.Internals
{
    internal static class CpfParser
    {
        private const string GeneralFormatValue = @"000\.000\.000\-00";
        private const string NewFormatValue = @"000000000\/00";
        private const string NumericFormatValue = @"00000000000";

        private const int FirstCheckDigitPosition = 9;
        private const int SecondCheckDigitPosition = 10;

        private static int[] FirstCheckDigitWeights =
            new int[] { 10, 9, 8, 7, 6, 5, 4, 3, 2 };
        
        private static int[] SecondCheckDigitWeights =
            new int[] { 11, 10, 9, 8, 7, 6, 5, 4, 3, 2 };
        
        private enum ParseExceptionKind
        {
            None,
            Argument,
            Format
        }

        private struct ParseResult
        {
            internal long result;
            internal string paramName;
            internal ParseExceptionKind exceptionKind;

            internal bool Succeed => exceptionKind == ParseExceptionKind.None;

            internal Exception GetException()
            {
                switch (exceptionKind)
                {
                    case ParseExceptionKind.Argument:
                        return new ArgumentException(string.Format(SR.ArgumentException_InvalidCpfNumber,
                            paramName));
                    
                    case ParseExceptionKind.Format:
                        return new FormatException(SR.FormatException_InvalidCpfFormat);

                    case ParseExceptionKind.None:   
                    default:
                        return new InvalidOperationException();
                };
            }
        }

        public static Cpf Parse(string s)
        {
            var pr = new ParseResult();
            pr.paramName = nameof(s);

            TryParseCpf(ref pr, s);

            if (pr.Succeed)
            {
                return new Cpf(pr.result);
            }

            throw pr.GetException();
        }

        public static bool TryParse(string s, out Cpf result)
        {
            var pr = new ParseResult();
            pr.paramName = nameof(s);

            TryParseCpf(ref pr, s);

            result = new Cpf(pr.result);
            return pr.Succeed;
        }

        private static void TryParseCpf(ref ParseResult pr, ReadOnlySpan<char> src)
        {
            if (src.Length == 0)
            {
                pr.exceptionKind = ParseExceptionKind.Argument;
                return;
            }

            ReadOnlySpan<char> fmt = GetFormat(src);

            if (fmt.Length < src.Length)
            {
                pr.exceptionKind = ParseExceptionKind.Format;
                return;
            }

            Span<char> dst = new char[11];

            for (int fmti = 0, srci = 0, dsti = 0; fmti < fmt.Length; fmti++)
            {
                if (fmt[fmti] == '0')
                {
                    if (Char.IsDigit(src[srci]))
                    {
                        dst[dsti++] = src[srci++];
                        continue;
                    }
                    Debug.WriteLine("FAIL: character '{0}' isn't decimal", src[srci]);
                    pr.exceptionKind = ParseExceptionKind.Format;
                    return;
                }
                else if (fmt[fmti] == '\\')
                {
                    if (fmt[++fmti] == src[srci++])
                    {
                        continue;
                    }
                    Debug.WriteLine("FAIL: unexpected character '{0}' after skip", src[--srci]);
                    pr.exceptionKind = ParseExceptionKind.Format;
                    return;
                }
                throw new InvalidOperationException();
            }

            if (!long.TryParse(dst, out long number))
            {
                Debug.WriteLine("FAIL: not a number");
                pr.exceptionKind = ParseExceptionKind.Format;
                return;
            }

            if (Char.GetNumericValue(dst[FirstCheckDigitPosition]) != 
                DigitChecker.Get(dst, FirstCheckDigitWeights))
            {
                Debug.WriteLine($"FAIL: DC1 isn't {DigitChecker.Get(dst, FirstCheckDigitWeights)}");
                pr.exceptionKind = ParseExceptionKind.Argument;
                return;
            }
                    
            if (Char.GetNumericValue(dst[SecondCheckDigitPosition]) != 
                DigitChecker.Get(dst, SecondCheckDigitWeights))
            {
                Debug.WriteLine($"FAIL: DC2 isn't {DigitChecker.Get(dst, SecondCheckDigitWeights)}");
                pr.exceptionKind = ParseExceptionKind.Argument;
                return;
            }

            pr.result = number;
        }

        private static ReadOnlySpan<char> GetFormat(ReadOnlySpan<char> src)
        {
            if (src.IndexOf('.') > 0 && src.IndexOf('-') > 0) 
            {
                Debug.WriteLine("Format detected: GENERAL");
                return GeneralFormatValue;
            }
            
            if (src.IndexOf('/') > 0)
            {
                Debug.WriteLine("Format detected: NEW");
                return NewFormatValue;
            }
            
            Debug.WriteLine("Format detected: NUMERIC");
            return NumericFormatValue;
        }
    }
}