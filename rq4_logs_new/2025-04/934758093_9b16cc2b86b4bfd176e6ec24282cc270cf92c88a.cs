using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LanguageSchoolApp.exceptions.Courses
{
    public enum CourseDropoutRequestExceptionType
    { 
        DropoutRequestExists,
        DropoutRequestNotFound
    }

    public class CourseDropoutRequestException : Exception
    {
        public string Text { get; set; }
        public CourseDropoutRequestExceptionType Type { get; set; }

        public CourseDropoutRequestException(string _text, CourseDropoutRequestExceptionType _type)
        { 
            Text = _text;
            Type = _type;
        }
    }
}