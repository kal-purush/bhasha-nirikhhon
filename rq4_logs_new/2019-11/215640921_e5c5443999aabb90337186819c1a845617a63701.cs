using BancoDeAlimentos.Entities;
using BancoDeAlimentos.Entities.Enum;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace BancoDeAlimentos.DTOs
{
    public class DeliveryDto
    {

        public string Key { get; set; }


        [Required]
        public Organization Organization { get; set; }
        public IEnumerable<ProductDeliveryDto> ProductDeliverys { get; set; }
        public DateTime EstimatedDate { get; set; }

        public DateTime EffectiveDate { get; set; }

        public string Status { get; set; }

        public bool ExpiredProducts { get; set; }

        public string productsToShow { get; set; }
        
    }
}