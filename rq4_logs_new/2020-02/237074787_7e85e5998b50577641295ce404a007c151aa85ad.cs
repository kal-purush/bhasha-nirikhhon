using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MailSender.lib.Service
{
    public static class TextEncoder
    {
        public static string Encode(this string Source, int Key = 1)
        {
            return new string(Source.Select(c => (char)(c + 1)).ToArray());
        }
        public static string Decode(this string Source, int Key = 1)
        {
            return new string(Source.Select(c => (char)(c - 1)).ToArray());
        }
    }
}