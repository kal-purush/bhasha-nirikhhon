using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace Covid19TemperatureAPI.Entities.Data
{
    [Table("TemperatureRecords")]
    public class TemperatureRecord
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int TemperatureRecordId { get; set; }


        public string PersonUID { get; set; }
        public string PersonName { get; set; }

        [ForeignKey("Device")]
        public string DeviceId { get; set; }
        public virtual Device Device { get; set; }

        [Column(TypeName = "decimal(5,3)")]
        [Required]
        public decimal Temperature { get; set; }

        public DateTime Timestamp { get; set; }

        public string ImagePath { get; set; }

    }
}