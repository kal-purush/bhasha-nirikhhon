using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TAB_clinic_Model
{
    public enum ExamKind
    {
        Physical,
        Lab
    }

    public static class ExamKindMethods
    {
        public static string? KindToStr(ExamKind kind)
        {
            return kind switch
            {
                ExamKind.Physical => "p",
                ExamKind.Lab => "l",
                _ => throw new ArgumentOutOfRangeException()
            };
        }

        public static ExamKind StrToKind(string kindStr)
        {
            return kindStr switch
            {
                "p" => ExamKind.Physical,
                "l" => ExamKind.Lab,
                _ => throw new ArgumentOutOfRangeException()
            };
        }

    }
}