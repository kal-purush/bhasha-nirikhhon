using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProyectoSourceCol.Clases.Punto4
{
    internal class Car
    {
        private string Brand;
        private int Model;
        private string Color;

        public Car(String brand, int model, string color) //constructor
        {
            this.Brand = brand;
            this.Model = model;
            this.Color = color;
        }
        public object getModel() => this.Model; // retorna el valor del modelo

        public object getBrand() => this.Brand; // retorna el valor del brand

        public object getColor() => this.Color; // retorna el valor del color

        /* Mientras no sea necesario, no se tienen que crear el resto de gets y sets */
    }
}