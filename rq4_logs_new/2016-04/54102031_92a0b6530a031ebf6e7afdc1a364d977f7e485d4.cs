using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ICT4Rails.Models.Enums;

namespace ICT4Rails.Models
{
    class Maintenance
    {
        private Tram tram { get; set; }
        public MaintenanceType Type { get; set; }
        public int Date { get; set; }
        public int Duration { get; set; }

        public Maintenance(MaintenanceType type, int date, int duration)
        {
            this.Type = type;
            this.Date = date;
            this.Duration = duration;
        }
    }
}