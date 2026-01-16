using ClientNexus.Domain.Enums;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClientNexus.Application.DTOs
{
    public class SocialAuthDTO
    {
        [Required]
        public string AccessToken { get; set; }
        [Required]
        public string Provider { get; set; }
        [Required]
        public UserType UserType { get; set; }

        [Required]
        public DateOnly BirthDate { get; set; }
        [Required]
        public string PhoneNumber { get; set; }
        public List<string>? PhoneNumbers { get; set; }
        [Required]
        public Gender Gender { get; set; }
        
        public IFormFile? MainImage { get; set; } 
      
        public List<AddressDTO>? Addresses { get; set; }
        public IFormFile? ImageIDUrl { get; set; }
        public IFormFile? ImageNationalIDUrl { get; set; }

        public string? Description { get; set; }
        public int? TypeId { get; set; }
        public int? YearsOfExperience { get; set; }
        public int? Office_consultation_price { get; set; }

        public int? Telephone_consultation_price { get; set; }
        public int? main_specializationID { get; set; }
        public List<int>? SpecializationIDS { get; set; }

    }
}