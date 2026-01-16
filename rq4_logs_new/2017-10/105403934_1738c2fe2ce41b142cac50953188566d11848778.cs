using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Collector2.Model
{
    public class Hardware
    {
        [Key, ForeignKey("Item")]
        public int ItemId { get; set; }
        public string Notes { get; set; }
        [Required, ForeignKey("HardwareSpec")]
        public int HardwareSpecId { get; set; }

        public virtual Item Item { get; set; }
        public virtual HardwareSpec HardwareSpec { get; set; }
    }
}