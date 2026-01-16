using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace A_Deux
{
    class Cliente
    {
        public string id{get; set;}
        public string estado_id{}
        public string nombre{get; set;}
        public string apellidos{get; set;}
        public string telefono{get; set;}
        public string email { get; set; }
    }
    public Cliente(){}
    public Cliente(){int pid, string pestado_id, string pnombre, string papellido, string ptelefono, string pemail}{
     this.id =pid;
    this.estado_id =pestado_id;
    this.nombre =pnombre;
    this.apellidos =papellidos;
    this.telefono =ptelefono;
    this.email =pemail;
    }
}