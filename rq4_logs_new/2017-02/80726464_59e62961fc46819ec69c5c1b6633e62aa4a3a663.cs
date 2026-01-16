using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AdminApp.Model
{
    class GestionHopital
    {
        public String Id { get; set; }
        public String nom{ get; set;}
        public String adresse { get; set; }
        public String service { get; set; }
        public String type { get; set; }
        public Double latitude { get; set; }
        public Double langitude { get; set; }

    }
}