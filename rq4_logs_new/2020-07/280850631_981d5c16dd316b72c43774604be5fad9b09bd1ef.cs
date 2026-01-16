using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace Covid19TemperatureAPI.Entities.Data
{
    [Table("Floors")]
    public class Floor
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int FloorId { get; set; }
        
        public string FloorNumber { get; set; }
        public string FloorDetails { get; set; }

        public string AdditionalDetails { get; set; }

        [ForeignKey("Building")]
        public int BuildingId { get; set; }
        public virtual Building Building { get; set; }
    }
}