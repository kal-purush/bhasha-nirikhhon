using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace CaptoneProject_IOTS_BOs.Models
{
    public partial class DeviceSpecification
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { set; get; }

        public string Name { set; get; } = "";

        [ForeignKey(nameof(IotsDevice))]
        public int IotDeviceId { set; get; }

        public DateTime CreatedDate { set; get; } = DateTime.Now;
        public virtual IotsDevice IotsDevice { set; get; }

        [JsonIgnore]
        public virtual IEnumerable<DeviceSpecificationsItem>? DeviceSpecificationsItems { set; get; }
    }
}