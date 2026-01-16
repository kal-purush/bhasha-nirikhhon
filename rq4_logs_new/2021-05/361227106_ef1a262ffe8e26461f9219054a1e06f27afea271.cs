using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TAB_clinic_Data.Database;

namespace TAB_clinic_Model
{
    public static class VisitManager
    {
        /// <summary>
        /// Returns the doctor's visit, filtering them with 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="doctor"></param>
        /// <param name="surnameFilter">filter for the user's name, doesn't filter</param>
        /// <param name="date"></param>
        /// <returns></returns>
        public static List<VisitModel> GetDoctorsVisits(WrappedContext db, Doctor doctor, string surnameFilter, DateTime? date)
        {

            System.Linq.IQueryable<Visit> visits;
            // lazy loading
            db.Context.Entry(doctor).Collection(u => u.Visits).Load();
            foreach(Visit v in doctor.Visits.ToList())
            {
                db.Context.Entry(v).Reference(r => r.IdPatientNavigation).Load();
            }

            if (date.HasValue)
            {
                visits = from v in doctor.Visits.AsQueryable()
                         where date.Value.Date.Equals(v.DtRegistered.Date) &&
                         v.IdPatientNavigation.Lastname.StartsWith(surnameFilter)
                         select v;
            }
            else
            {
                visits = from v in doctor.Visits.AsQueryable()
                         where v.IdPatientNavigation.Lastname.StartsWith(surnameFilter)
                         select v;
            }

            List<VisitModel> ret = new();
            foreach(Visit v in visits.ToList())
                ret.Add(new VisitModel(v));

            return ret;
        }

    }
}