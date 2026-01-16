using LogicaNegocio.Dominio;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WebMVC.Models
{
    public class SeleccionViewModel
    {
        public IEnumerable<Grupo> Grupos { get; set; }
        public IEnumerable<Pais> Paises { get; set; }
        public Seleccion seleccion { get; set; }
        public string NombreSeleccion { get; set; }
        public int IdGrupo { get; set; }
        public int IdPaisSeleccionado { get; set; }
        public int IdGrupoSeleccionado { get; set; }


    }
}