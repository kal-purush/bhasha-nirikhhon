using BancoDeAlimentos.Entities.Enum;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace BancoDeAlimentos.Entities
{
    public class Product
    {
        public Product()
        {
            Key = Guid.NewGuid().ToString();
        }

        [Key]
        public long Id { get; set; }
        public string Key { get; set; }


        [Required]
        public string Name { get; set; }
        public string Description { get; set; }
        public int Stock { get; set; }
    }
}