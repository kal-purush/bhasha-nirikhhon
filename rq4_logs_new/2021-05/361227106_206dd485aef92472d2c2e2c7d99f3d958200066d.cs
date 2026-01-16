using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using TAB_clinic_Data.Database;

namespace TAB_clinic_Model
{
    public static class PatientMangager
    {
        public static PatientModel? FindPatient(WrappedContext db, string pesel)
        {
            var patient = db.Context.Patients
                .Where(p => p.Pesel == pesel)
                .FirstOrDefault();

            if (patient is null)
            {
                return null;
            }

            return new PatientModel(patient);
        }

        public static void CreatePatient(WrappedContext db, string name, string lastname, string pesel)
        {
            if (FindPatient(db, pesel) != null)
            {
                Trace.WriteLine($"Patient with pesel '{pesel}' already exists");
                throw new UserAlreadyExistsException(pesel);
            }

            var context = db.Context;

            var newPatient = new PatientModel(db)
            {
                Name = name,
                Lastname = lastname,
                Pesel = pesel
            };

            context.SaveChanges();
        }
        public static List<PatientModel> GetPatients(WrappedContext db)
        {
            var patients = db.Context.Patients.ToList();
            var patientList = new List<PatientModel>();

            foreach (var patient in patients)
            {
                patientList.Add(new PatientModel(patient));
            }

            return patientList;
        }
    }
}