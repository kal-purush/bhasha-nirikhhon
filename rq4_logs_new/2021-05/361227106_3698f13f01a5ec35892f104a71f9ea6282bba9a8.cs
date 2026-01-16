using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using static TAB_clinic_Model.ExamKindMethods;
using TAB_clinic_Data.Database;

namespace TAB_clinic_Model
{
    public class ExamTypeModel
    {
        private readonly ExamType dbType;

        internal ExamTypeModel(ExamType examType)
        {
            this.dbType = examType;
        }

        public string Code
        {
            get => this.dbType.Code;
        }

        public string Name
        {
            get => this.dbType.Name;
        }

        public ExamKind Kind
        {
            get => StrToKind(this.dbType.Type);
        }

        public override string ToString()
        {
            return this.Name;
        }

        public static ExamTypeModel? FindExamType(WrappedContext db, string code)
        {
            var examType = db.Context.ExamTypes
                             .Where(et => et.Code == code)
                             .FirstOrDefault();

            if (examType is null)
                return null;

            return new ExamTypeModel(examType);

        }

        public static ExamTypeModel[] GetPhysicalExamTypes(WrappedContext db)
        {
            return db.Context.ExamTypes.Where(et => et.Type == ExamKind.Physical.KindToStr())
                                       .Select(et => new ExamTypeModel(et)).ToArray();
        }

        public static ExamTypeModel[] GetLabExamTypes(WrappedContext db)
        {
            return db.Context.ExamTypes.Where(et => et.Type == ExamKind.Lab.KindToStr())
                                       .Select(et => new ExamTypeModel(et)).ToArray();
        }
    }
}