using System;
using System.Collections.Generic;

#nullable disable

namespace IoT_Environment.Models
{
    public partial class Device
    {
        public Device()
        {
            Reports = new HashSet<Report>();
        }

        public int Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public string Address { get; set; }
        public DateTime DateRegistered { get; set; }
        public string ConnectionType { get; set; }
        public bool? Active { get; set; }
        public int? Relay { get; set; }

        public virtual Relay RelayNavigation { get; set; }
        public virtual ICollection<Report> Reports { get; set; }
    }
}