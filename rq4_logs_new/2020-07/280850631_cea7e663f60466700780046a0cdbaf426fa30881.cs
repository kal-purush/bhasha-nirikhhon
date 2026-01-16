using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace Covid19TemperatureAPI.Entities.Data
{
    [Table("MaskRecords")]
    public class MaskRecord
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int MaskRecordId { get; set; }


        public string PersonUID { get; set; }
        public string PersonName { get; set; }

        [ForeignKey("Device")]
        public string DeviceId { get; set; }
        public virtual Device Device { get; set; }

        [Required]
        public int MaskValue { get; set; }

        public DateTime Timestamp { get; set; }

        public string ImagePath { get; set; }

    }
}