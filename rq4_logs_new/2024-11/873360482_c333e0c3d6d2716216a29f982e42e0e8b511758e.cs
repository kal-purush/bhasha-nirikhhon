using HairSalon.Core.Entities;
using HairSalon.Core.Enums;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HairSalon.Core.Dtos.Requests
{
    public class AppointmentUpdateModel
    {
        public int Id { get; set; }
        public int CustomerId { get; set; }

        public int StylistId { get; set; }

        public DateOnly AppointmentDate { get; set; }

        public TimeOnly StartTime { get; set; }

        public TimeOnly EndTime { get; set; }

        [StringLength(255)]
        public string? Note { get; set; }

        public AppointmentStatus AppointmentStatus { get; set; }

        public ICollection<AppointmentService> AppointmentServices { get; set; } = new List<AppointmentService>();

        //public ICollection<Transaction> Transactions { get; set; } = new List<Transaction>();
    }
}