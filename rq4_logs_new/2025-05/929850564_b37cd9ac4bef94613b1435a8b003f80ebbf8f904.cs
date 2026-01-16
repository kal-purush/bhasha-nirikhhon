using ClientNexus.Domain.Entities.Services;
using ClientNexus.Domain.Entities.Users;
using ClientNexus.Domain.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClientNexus.Domain.Entities.Others
{
    public class AvailableDay
    {
        public int Id { get; set; }
        public int ServiceProviderId { get; set; }
        public DayOfWeek DayOfWeek { get; set; }
        public TimeSpan StartTime { get; set; }
        public TimeSpan EndTime { get; set; }
        public TimeSpan SlotDuration { get; set; }
        public SlotType SlotType { get; set; }
        public DateTime? LastGenerationEndDate { get; set; }
        public ServiceProvider? ServiceProvider { get; set; }
        public ICollection<Slot>? Slots { get; set; }
    }
}