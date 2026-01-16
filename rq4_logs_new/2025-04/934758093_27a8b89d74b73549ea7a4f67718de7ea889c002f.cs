using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LanguageSchoolApp.exceptions.Users
{
    public enum PenaltyPointExceptionType
    { 
        PenaltyPointExists,
        PenaltyPointNotFound
    }

    public class PenaltyPointException : Exception
    {
        public string Text { get; set; }
        public PenaltyPointExceptionType Type { get; set; }

        public PenaltyPointException(string _text, PenaltyPointExceptionType _type)
        { 
            Text = _text;
            Type = _type;
        }
    }
}