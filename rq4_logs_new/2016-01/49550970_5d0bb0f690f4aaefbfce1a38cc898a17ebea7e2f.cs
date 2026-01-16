using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestProject.Models.DBModels
{
    public class Event
    {
        [Key]
        public Guid Id { get; set; }
        public string EventName { get; set; }
        public string City { get; set; }
        public Address Address { get; set; }
        public DateTime EventStartTime { get; set; }
        public DateTime EventEndTime { get; set; }
        public string Description { get; set; }

        [ForeignKey("District")]
        public Guid DistrictId { get; set; }
        public virtual District District { get; set; }
        public virtual ICollection<EventImage> EventImage { get; set; }
        public virtual ICollection<EventVideo> EventVideo { get; set; }
        public virtual ICollection<EventComment> EventComment { get; set; }
    }
}