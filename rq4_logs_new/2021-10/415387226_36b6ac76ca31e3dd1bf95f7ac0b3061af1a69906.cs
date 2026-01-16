using System;
using System.Collections.Generic;
using Home2Med.Shared.Entity;

namespace Home2Med.Client.Services
{
    public class ServiceNurse : IServiceNurse
    {
        public List<Nurse> GetNurses()
        {
            return new List<Nurse>()
            {
                new Nurse()
                {
                    Id = 1,
                    NurseName = "Piedad Piedras",
                    NurseDocumentType = 1,
                    NurseDocument = "12345689",
                    NurseGender = 2,
                    NursePhone = "3104789632",
                    NurseSpeciality = 1,
                    NursePhoto = "/Images/enfermeras/fotoe01.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 2,
                    NurseName = "Benita Camelas",
                    NurseDocumentType = 2,
                    NurseDocument = "234567890",
                    NurseGender = 2,
                    NursePhone = "3171234789",
                    NurseSpeciality = 1,
                    NursePhoto = "/Images/enfermeras/fotoe02.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 3,
                    NurseName = "Cansona Masmamon",
                    NurseDocumentType = 3,
                    NurseDocument = "345678901",
                    NurseGender = 2,
                    NursePhone = "3140143694",
                    NurseSpeciality = 1,
                    NursePhoto = "/Images/enfermeras/fotoe03.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 4,
                    NurseName = "Teresa Arrieta",
                    NurseDocumentType = 3,
                    NurseDocument = "456789012",
                    NurseGender = 2,
                    NursePhone = "301010112",
                    NurseSpeciality = 2,
                    NursePhoto = "/Images/enfermeras/fotoe04.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 5,
                    NurseName = "Carla Arrieta",
                    NurseDocumentType = 2,
                    NurseDocument = "456789012",
                    NurseGender = 2,
                    NursePhone = "301010114",
                    NurseSpeciality = 2,
                    NursePhoto = "/Images/enfermeras/fotoe05.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 6,
                    NurseName = "María Segura",
                    NurseDocumentType = 1,
                    NurseDocument = "456789012",
                    NurseGender = 2,
                    NursePhone = "301010112",
                    NurseSpeciality = 2,
                    NursePhoto = "/Images/enfermeras/fotoe06.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 7,
                    NurseName = "Marcela Segura",
                    NurseDocumentType = 1,
                    NurseDocument = "456789012",
                    NurseGender = 2,
                    NursePhone = "301010112",
                    NurseSpeciality = 1,
                    NursePhoto = "/Images/enfermeras/fotoe07.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 8,
                    NurseName = "Dominique Nique Nique",
                    NurseDocumentType = 1,
                    NurseDocument = "456789012",
                    NurseGender = 2,
                    NursePhone = "301010112",
                    NurseSpeciality = 3,
                    NursePhoto = "/Images/enfermeras/fotoe08.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 9,
                    NurseName = "Elena Caiman Sanchez",
                    NurseDocumentType = 2,
                    NurseDocument = "456789012",
                    NurseGender = 2,
                    NursePhone = "301010112",
                    NurseSpeciality = 2,
                    NursePhoto = "/Images/enfermeras/fotoe09.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 10,
                    NurseName = "Martina Emilia Cochise",
                    NurseDocumentType = 3,
                    NurseDocument = "456789012",
                    NurseGender = 2,
                    NursePhone = "301010112",
                    NurseSpeciality = 2,
                    NursePhoto = "/Images/enfermeras/fotoe10.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 11,
                    NurseName = "Elena No Crece",
                    NurseDocumentType = 3,
                    NurseDocument = "456789012",
                    NurseGender = 2,
                    NursePhone = "301010112",
                    NurseSpeciality = 2,
                    NursePhoto = "/Images/enfermeras/fotoe11.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 12,
                    NurseName = "Morticia Monster",
                    NurseDocumentType = 2,
                    NurseDocument = "456789012",
                    NurseGender = 2,
                    NursePhone = "301010112",
                    NurseSpeciality = 1,
                    NursePhoto = "/Images/enfermeras/fotoe12.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 13,
                    NurseName = "Margarita Larrota",
                    NurseDocumentType = 1,
                    NurseDocument = "456774012",
                    NurseGender = 2,
                    NursePhone = "301017412",
                    NurseSpeciality = 1,
                    NursePhoto = "/Images/enfermeras/fotoe13.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 14,
                    NurseName = "Magdalena Largacha",
                    NurseDocumentType = 2,
                    NurseDocument = "987789012",
                    NurseGender = 2,
                    NursePhone = "301019875",
                    NurseSpeciality = 1,
                    NursePhoto = "/Images/enfermeras/fotoe14.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 15,
                    NurseName = "Virginia Tenjo",
                    NurseDocumentType = 1,
                    NurseDocument = "451599012",
                    NurseGender = 2,
                    NursePhone = "317510112",
                    NurseSpeciality = 1,
                    NursePhoto = "/Images/enfermeras/fotoe15.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 16,
                    NurseName = "María Melo Masmela",
                    NurseDocumentType = 1,
                    NurseDocument = "123489012",
                    NurseGender = 2,
                    NursePhone = "304571112",
                    NurseSpeciality = 2,
                    NursePhoto = "/Images/enfermeras/fotoe16.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 17,
                    NurseName = "Esperanza Rocio Manjarres",
                    NurseDocumentType = 2,
                    NurseDocument = "745789012",
                    NurseGender = 2,
                    NursePhone = "310101121",
                    NurseSpeciality = 3,
                    NursePhoto = "/Images/enfermeras/fotoe17.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 18,
                    NurseName = "Blanca Negrette",
                    NurseDocumentType = 1,
                    NurseDocument = "487785012",
                    NurseGender = 2,
                    NursePhone = "3150104112",
                    NurseSpeciality = 4,
                    NursePhoto = "/Images/enfermeras/fotoe18.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 19,
                    NurseName = "Natalia Mejia",
                    NurseDocumentType = 3,
                    NurseDocument = "125458901",
                    NurseGender = 2,
                    NursePhone = "3178010257",
                    NurseSpeciality = 1,
                    NursePhoto = "/Images/enfermeras/fotoe19.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 20,
                    NurseName = "Maruja Puerta Segura",
                    NurseDocumentType = 1,
                    NurseDocument = "197789012",
                    NurseGender = 2,
                    NursePhone = "3101127521",
                    NurseSpeciality = 2,
                    NursePhoto = "/Images/enfermeras/fotoe20.jpg",
                    NurseStatus = true
                },
                new Nurse()
                {
                    Id = 21,
                    NurseName = "Sofía Elena Marquez",
                    NurseDocumentType = 1,
                    NurseDocument = "124569012",
                    NurseGender = 2,
                    NursePhone = "3153101122",
                    NurseSpeciality = 3,
                    NursePhoto = "/Images/enfermeras/fotoe21.jpg",
                    NurseStatus = true
                }
            };
        }
    }
}