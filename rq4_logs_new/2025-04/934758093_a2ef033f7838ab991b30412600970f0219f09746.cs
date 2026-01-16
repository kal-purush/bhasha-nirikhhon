using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LanguageSchoolApp.exceptions.Exams
{
    public enum ExamApplicationExceptionType
    { 
        ExamApplicationNotFound,
        ExamApplicationAlreadyExists
    }

    public class ExamApplicationException : Exception
    {
        public string Text { get; set; }
        public ExamApplicationExceptionType Type { get; set; }

        public ExamApplicationException(string _text, ExamApplicationExceptionType _type) 
        { 
            Text = _text;
            Type = _type;
        }
    }
}