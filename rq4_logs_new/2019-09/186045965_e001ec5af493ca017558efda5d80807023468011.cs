using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using Singer.Models;

namespace Singer.DTOs.Users
{
   public class LinkedCareUserDTO : UserDTO
   {
      [Required]
      [DataType(DataType.Date)]
      [DisplayName("Geboortedatum")]
      public DateTime BirthDay { get; set; }

      [Required]
      [StringLength(maximumLength: 10,
         ErrorMessage = "Het {0} moet een lengte hebben van {2} karakters.",
         MinimumLength = 10)]
      [DisplayName("Dossiernummer")]
      public string CaseNumber { get; set; }

      [Required]
      [DisplayName("Leeftijdsgroep")]
      public AgeGroup AgeGroup { get; set; }

      [DisplayName("Klas/Extern")]
      public bool IsExtern { get; set; }

      [DisplayName("Traject")]
      public bool HasTrajectory { get; set; }

      [DisplayName("Opv. normaal")]
      public bool HasNormalDayCare { get; set; }

      [DisplayName("Opv. vakantie")]
      public bool HasVacationDayCare { get; set; }

      [DisplayName("Middelen")]
      public bool HasResources { get; set; }
   }
}