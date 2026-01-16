using System;
using System.Collections.Generic;
using System.Text;

namespace DTOs
{
    public class DTOSeleccion
    {
        public int Id { get; set; }
        public string Nombre { get; set; }
        public int Puntaje { get; set; }
        public int GolesAFavor { get; set; } = 0;
        public int GolesEnContra { get; set; } = 0;

        public int DiferenciaGoles { get; set; }

        public DTOSeleccion()
        {
            DiferenciaGoles = GolesAFavor - GolesEnContra;
        }

        public DTOSeleccion(int id, string nombre, int puntaje, int golesAFavor, int golesEnContra)
        {
            Id = id;
            Nombre = nombre;
            Puntaje = puntaje;
            GolesAFavor = golesAFavor;
            GolesEnContra = golesEnContra;
            DiferenciaGoles = golesAFavor - golesEnContra;
        }
    }
}