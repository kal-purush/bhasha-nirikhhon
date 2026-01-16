using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TAB_clinic_Model
{
    public class ExamManager
    {
        public static void CreatePhysicalExam(WrappedContext db, VisitModel visit, string result, string code)
        {
            var context = db.Context;
            var newExam = new PhysicalExamModel(db)
            {
                IdVisit = visit.IdVisit,
                Result = result,
                Code = code
            };

            context.SaveChanges();
        }
        public static void CreateLabExam(WrappedContext db, VisitModel visit, string doctorsNotes, string code)
        {
            var context = db.Context;
            var newExam = new LabExamModel(db)
            {
                IdVisit = visit.IdVisit,
                DoctorsNotes = doctorsNotes,
                Code = code
            };

            context.SaveChanges();
        }

        public static List<PhysicalExamModel> GetPhysicalExams(WrappedContext db)
        {
            return db.Context.PhysicalExams.Select(pe => new PhysicalExamModel(pe)).ToList();
        }

        public static List<LabExamModel> GetLabExams(WrappedContext db)
        {
            return db.Context.LabExams.Select(le => new LabExamModel(le)).ToList();
        }
    }
}