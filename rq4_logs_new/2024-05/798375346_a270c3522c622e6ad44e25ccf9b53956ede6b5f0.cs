using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Proyecto_Final
{
    internal class Cliente
    {
        private string nombre;
        private string apellido;
        private string tipoAbastecimiento;
        private double cantidadAbastecer;
        private int bombaSeleccionada;
        private DateTime fecha;

        public string Nombre { get => nombre; set => nombre = value; }
        public string Apellido { get => apellido; set => apellido = value; }
        public string TipoAbastecimiento { get => tipoAbastecimiento; set => tipoAbastecimiento = value; }
        public double CantidadAbastecer { get => cantidadAbastecer; set => cantidadAbastecer = value; }
        public int BombaSeleccionada { get => bombaSeleccionada; set => bombaSeleccionada = value; }
        public DateTime Fecha { get => fecha; set => fecha = value; }
    }
}