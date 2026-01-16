using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace Covid19TemperatureAPI.Entities.Data
{
    [Table("Buildings")]
    public class Building
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int BuildingId { get; set; }
        public string BuildingName { get; set; }

        public string BuildingDescription { get; set; }

        [ForeignKey("SiteId")]
        public int SiteId { get; set; }
        public virtual Site Site { get; set; }


    }
}