namespace COP4331_RestaurantSystem_WebAPI.Models
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Data.Entity.Spatial;

    public partial class MenuItem
    {
        [Key]
        public int ID { get; set; }

        [Required]
        [StringLength(256)]
        public string Name { get; set; }

        [Required]
        public int Category { get; set; }

        [Column(TypeName = "money")]
        public decimal Price { get; set; }
    }
}