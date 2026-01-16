using System;
using System.Collections.Generic;
using Home2Med.Shared.Entity;

namespace Home2Med.Client.Services
{
    public class ServicePatient : IServicePatient
    {
        public List<Patient> GetPatients()
        {
            return new List<Patient>()
            {
                new Patient()
                {
                    Id = 1,
                    PatientName = "Camila Piedras",
                    PatientDocumentType = 1,
                    PatientDocument = "12345689",
                    PatientGender = 2,
                    PatientPhone = "3104789632",
                    PatientEPS = "Sanitas",
                    PatientAddress = "Calle 156 # 12-40 Casa 1",
                    PatientGeorreferentiation = "4.738849039804605, -74.04270308868264",
                    PatientDiagnosis = 1,
                    PatientDoctor = 2,
                    PatientPhoto = "/Images/pacientes/fotop01.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 2,
                    PatientName = "Sonia Zua",
                    PatientDocumentType = 1,
                    PatientDocument = "1365452",
                    PatientGender = 2,
                    PatientPhone = "31065894226",
                    PatientEPS = "Compensar",
                    PatientAddress = "Calle 135 # 12-36 Casa 84",
                    PatientGeorreferentiation = "4.738842039804605, -73.04270308868264",
                    PatientDiagnosis = 2,
                    PatientDoctor = 2,
                    PatientPhoto = "/Images/pacientes/fotop02.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 3,
                    PatientName = "Mayerly Pérez",
                    PatientDocumentType = 3,
                    PatientDocument = "36594852",
                    PatientGender = 2,
                    PatientPhone = "3115428469",
                    PatientEPS = "Sanitas",
                    PatientAddress = "Calle 12b # 12-40 Apto 2349",
                    PatientGeorreferentiation = "4.738842039804605, -72.04270308868264",
                    PatientDiagnosis = 3,
                    PatientDoctor = 4,
                    PatientPhoto = "/Images/pacientes/fotop03.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 4,
                    PatientName = "Carlos Contreras",
                    PatientDocumentType = 1,
                    PatientDocument = "11125468",
                    PatientGender = 1,
                    PatientPhone = "3125789632",
                    PatientEPS = "Nueva EPS",
                    PatientAddress = "Carrera 156B # 11-40",
                    PatientGeorreferentiation = "4.738842039804605, -73.04270308868264",
                    PatientDiagnosis = 4,
                    PatientDoctor = 1,
                    PatientPhoto = "/Images/pacientes/fotop04.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 5,
                    PatientName = "Roberto Escobar",
                    PatientDocumentType = 1,
                    PatientDocument = "1015423015",
                    PatientGender = 1,
                    PatientPhone = "3104768542",
                    PatientEPS = "Sanitas",
                    PatientAddress = "Avenida 18 # 34f-40 Casa 1",
                    PatientGeorreferentiation = "4.738842039804605, -73.04270308868264",
                    PatientDiagnosis = 4,
                    PatientDoctor = 4,
                    PatientPhoto = "/Images/pacientes/fotop05.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 6,
                    PatientName = "Camila Peralta",
                    PatientDocumentType = 1,
                    PatientDocument = "20568354",
                    PatientGender = 2,
                    PatientPhone = "31269854758",
                    PatientEPS = "Compensar",
                    PatientAddress = "Avenida circunvalar # 2-10",
                    PatientGeorreferentiation = "4.838842039804605, -72.04270308868264",
                    PatientDiagnosis = 4,
                    PatientDoctor = 2,
                    PatientPhoto = "/Images/pacientes/fotop06.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 7,
                    PatientName = "Marcelo Segura",
                    PatientDocumentType = 1,
                    PatientDocument = "16358942",
                    PatientGender = 1,
                    PatientPhone = "3165849632",
                    PatientEPS = "Sanitas",
                    PatientAddress = "Calle 28f # 10-40 Casa 21",
                    PatientGeorreferentiation = "5.738842039804605, -71.04270308868264",
                    PatientDiagnosis = 3,
                    PatientDoctor = 1,
                    PatientPhoto = "/Images/pacientes/fotop07.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 8,
                    PatientName = "Pedro Gomez Nique",
                    PatientDocumentType = 2,
                    PatientDocument = "635894",
                    PatientGender = 1,
                    PatientPhone = "31425648596",
                    PatientEPS = "Sura",
                    PatientAddress = "Carrera 23 # 12-40",
                    PatientGeorreferentiation = "4.738842039804605, -73.04270308868264",
                    PatientDiagnosis = 2,
                    PatientDoctor = 2,
                    PatientPhoto = "/Images/pacientes/fotop08.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 9,
                    PatientName = "Berta Sanchez",
                    PatientDocumentType = 1,
                    PatientDocument = "658452254",
                    PatientGender = 2,
                    PatientPhone = "3104789782",
                    PatientEPS = "Sanitas",
                    PatientAddress = "Carrera 26 # 12-40 Apto 103, Int 2",
                    PatientGeorreferentiation = "4.938842039804605, -71.04270308868264",
                    PatientDiagnosis = 1,
                    PatientDoctor = 3,
                    PatientPhoto = "/Images/pacientes/fotop09.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 10,
                    PatientName = "Elena Cochise",
                    PatientDocumentType = 1,
                    PatientDocument = "12658489",
                    PatientGender = 2,
                    PatientPhone = "3165789632",
                    PatientEPS = "Sanitas",
                    PatientAddress = "Calle 156 # 112-40 Casa 1",
                    PatientGeorreferentiation = "4.838842039804605, -72.04270308868264",
                    PatientDiagnosis = 1,
                    PatientDoctor = 2,
                    PatientPhoto = "/Images/pacientes/fotop10.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 11,
                    PatientName = "Clara Pérez",
                    PatientDocumentType = 3,
                    PatientDocument = "125487",
                    PatientGender = 2,
                    PatientPhone = "316539632",
                    PatientEPS = "Cruz Blanca",
                    PatientAddress = "Calle 2 # 12-40 Casa 16",
                    PatientGeorreferentiation = "5.738842039804605, -72.36270308868264",
                    PatientDiagnosis = 2,
                    PatientDoctor = 2,
                    PatientPhoto = "/Images/pacientes/fotop11.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 12,
                    PatientName = "Camilo Basto",
                    PatientDocumentType = 1,
                    PatientDocument = "12635689",
                    PatientGender = 1,
                    PatientPhone = "3104789632",
                    PatientEPS = "Sanitas",
                    PatientAddress = "Calle 154 # 12-40 Casa 16",
                    PatientGeorreferentiation = "4.838842039804605, -73.04270308868264",
                    PatientDiagnosis = 3,
                    PatientDoctor = 2,
                    PatientPhoto = "/Images/pacientes/fotop12.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 13,
                    PatientName = "Anton González",
                    PatientDocumentType = 1,
                    PatientDocument = "1698542169",
                    PatientGender = 1,
                    PatientPhone = "3125358564",
                    PatientEPS = "Cruz Blanca",
                    PatientAddress = "Calle 36f # 156-40 Apto 1045",
                    PatientGeorreferentiation = "4.738842039804605, -73.04270308868264",
                    PatientDiagnosis = 4,
                    PatientDoctor = 2,
                    PatientPhoto = "/Images/pacientes/fotop13.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 14,
                    PatientName = "Camilo Contreras",
                    PatientDocumentType = 1,
                    PatientDocument = "163521689",
                    PatientGender = 1,
                    PatientPhone = "314526358",
                    PatientEPS = "Sanitas",
                    PatientAddress = "Carrera 21 # 112-94 sur Apto 103, Interior 12",
                    PatientGeorreferentiation = "6.738842039804605, -73.04270308868264",
                    PatientDiagnosis = 4,
                    PatientDoctor = 2,
                    PatientPhoto = "/Images/pacientes/fotop14.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 15,
                    PatientName = "Ermenegildo Suárez Jamoque",
                    PatientDocumentType = 2,
                    PatientDocument = "3652419",
                    PatientGender = 1,
                    PatientPhone = "3104799632",
                    PatientEPS = "Sisbén",
                    PatientAddress = "Calle 156h # 1-40",
                    PatientGeorreferentiation = "6.738842039804605, -73.04270308868264",
                    PatientDiagnosis = 3,
                    PatientDoctor = 2,
                    PatientPhoto = "/Images/pacientes/fotop15.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 16,
                    PatientName = "Camila Gaviria",
                    PatientDocumentType = 2,
                    PatientDocument = "1064561",
                    PatientGender = 2,
                    PatientPhone = "3123789632",
                    PatientEPS = "Sisbén",
                    PatientAddress = "Calle 1sur # 12-40",
                    PatientGeorreferentiation = "4.738842039804605, -73.04270308868264",
                    PatientDiagnosis = 2,
                    PatientDoctor = 3,
                    PatientPhoto = "/Images/pacientes/fotop16.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 17,
                    PatientName = "Camila Herrera",
                    PatientDocumentType = 1,
                    PatientDocument = "1014874521",
                    PatientGender = 2,
                    PatientPhone = "3125956547",
                    PatientEPS = "Sanitas",
                    PatientAddress = "Calle 156 # 14-46 Casa 6",
                    PatientGeorreferentiation = "6.738842039804605, -71.04270308868264",
                    PatientDiagnosis = 1,
                    PatientDoctor = 3,
                    PatientPhoto = "/Images/pacientes/fotop17.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 18,
                    PatientName = "Sofía Lorenz",
                    PatientDocumentType = 1,
                    PatientDocument = "163251689",
                    PatientGender = 2,
                    PatientPhone = "3112487546",
                    PatientEPS = "Compensar",
                    PatientAddress = "Calle 12 sur # 65b-152 Apto 1254 Int 3",
                    PatientGeorreferentiation = "5.738842039804605, -73.04270308868264",
                    PatientDiagnosis = 1,
                    PatientDoctor = 3,
                    PatientPhoto = "/Images/pacientes/fotop18.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 19,
                    PatientName = "Claudio Mejía",
                    PatientDocumentType = 3,
                    PatientDocument = "3654159",
                    PatientGender = 1,
                    PatientPhone = "3185241545",
                    PatientEPS = "Famisanar",
                    PatientAddress = "Avenida 124f # 1-29 Casa 12",
                    PatientGeorreferentiation = "4.738842039804605, -73.04270308868264",
                    PatientDiagnosis = 2,
                    PatientDoctor = 4,
                    PatientPhoto = "/Images/pacientes/fotop19.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 20,
                    PatientName = "Germán Peñuela",
                    PatientDocumentType = 1,
                    PatientDocument = "1018410356",
                    PatientGender = 1,
                    PatientPhone = "3125258469",
                    PatientEPS = "Sura",
                    PatientAddress = "Calle 20 # 5 -44",
                    PatientGeorreferentiation = "5.738842039804605, -72.04270308868264",
                    PatientDiagnosis = 3,
                    PatientDoctor = 2,
                    PatientPhoto = "/Images/pacientes/fotop20.png",
                    PatientStatus = true
                },
                new Patient()
                {
                    Id = 21,
                    PatientName = "Gregorio Zuleta Ocampo",
                    PatientDocumentType = 2,
                    PatientDocument = "3654218",
                    PatientGender = 1,
                    PatientPhone = "3102546325",
                    PatientEPS = "Sanitas",
                    PatientAddress = "Calle 112 # 1-65 Apto 1203",
                    PatientGeorreferentiation = "4.738842039804605, -73.04270308868264",
                    PatientDiagnosis = 4,
                    PatientDoctor = 2,
                    PatientPhoto = "/Images/pacientes/fotop21.png",
                    PatientStatus = true
                }
            };
        }
    }
}