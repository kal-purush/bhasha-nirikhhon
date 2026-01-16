using HospitalMap.WPF.ModelWPF;
using Model.AllActors;
using System;
using System.Collections.Generic;
using System.Text;

namespace HospitalMap.WPF.Converter
{
    class PatientConverter: AbstractConverter
    {

        public static PatientView ConvertPatientToPatientView(Patient patient)
    {
        return new PatientView
        {
            NameOfPatient = patient.Name,
            SurnameOfPatient = patient.Surname,
            IdOfPatient = patient.Id,
            JmbgOfPatient=patient.Jmbg

        };
    }

    public static IList<PatientView> ConvertPatientToPatientViewList(IList<Patient> patient)
        => ConvertEntityListToViewList(patient, ConvertPatientToPatientView);
}
}