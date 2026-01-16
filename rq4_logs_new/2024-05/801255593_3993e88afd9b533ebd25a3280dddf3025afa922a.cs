using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ClasesBase
{
    public class Atleta
    {
        private int atl_ID;
        private string atl_DNI;
        private string atl_Apellido;
        private string atl_Nombre;
        private string atl_Nacionalidad;
        private string atl_Entrenador;
        private char atl_Genero;
        private double atl_Altura;
        private double atl_Peso;
        private DateTime atl_FechaNac;
        private string atl_Direccion;
        private string atl_Email;

        public int Atl_ID { get => atl_ID; }
        public string Atl_DNI { get => atl_DNI; set => atl_DNI = value; }
        public string Atl_Apellido { get => atl_Apellido; set => atl_Apellido = value; }
        public string Atl_Nombre { get => atl_Nombre; set => atl_Nombre = value; }
        public string Atl_Nacionalidad { get => atl_Nacionalidad; set => atl_Nacionalidad = value; }
        public string Atl_Entrenador { get => atl_Entrenador; set => atl_Entrenador = value; }
        public char Atl_Genero { get => atl_Genero; set => atl_Genero = value; }
        public double Atl_Altura { get => atl_Altura; set => atl_Altura = value; }
        public double Atl_Peso { get => atl_Peso; set => atl_Peso = value; }
        public DateTime Atl_FechaNac { get => atl_FechaNac; set => atl_FechaNac = value; }
        public string Atl_Direccion { get => atl_Direccion; set => atl_Direccion = value; }
        public string Atl_Email { get => atl_Email; set => atl_Email = value; }

        public Atleta()
        {
        }
        public Atleta(int atl_ID, string atl_DNI, string atl_Apellido, string atl_Nombre, string atl_Nacionalidad, string atl_Entrenador, char atl_Genero, double atl_Altura, double atl_Peso, DateTime atl_FechaNac, string atl_Direccion, string atl_Email)
        {
            this.atl_ID = atl_ID;
            this.atl_DNI = atl_DNI;
            this.atl_Apellido = atl_Apellido;
            this.atl_Nombre = atl_Nombre;
            this.atl_Nacionalidad = atl_Nacionalidad;
            this.atl_Entrenador = atl_Entrenador;
            this.atl_Genero = atl_Genero;
            this.atl_Altura = atl_Altura;
            this.atl_Peso = atl_Peso;
            this.atl_FechaNac = atl_FechaNac;
            this.atl_Direccion = atl_Direccion;
            this.atl_Email = atl_Email;
        }
    }
}