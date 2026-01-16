using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace Covid19TemperatureAPI.Entities.Data
{
    [Table("Gates")]
    public class Gate
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int GateId { get; set; }

        public string GateNumber { get; set; }
        public string AdditionalDetails { get; set; }

        [ForeignKey("Floor")]
        public int FloorId { get; set; }
        public virtual Floor Floor { get; set; }

    }
}