using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace eDnevnikDev.Models
{
    public class ArhivaKreiranjaOdeljenja
    {
        public int Id { get; set; }
        public byte Razred { get; set; }

        public int OdeljenjeId{ get; set; }
        public virtual Odeljenje Odeljenje { get; set; }

        public DateTime VremePromeneStatusa{ get; set; }

        public int StatusId { get; set; }
        public virtual StatusOdeljenje Status { get; set; }


    }
}