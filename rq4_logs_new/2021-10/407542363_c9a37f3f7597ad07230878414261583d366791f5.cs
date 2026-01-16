using System;
using System.Collections.Generic;
using System.Text;

namespace Library
{
    public class BadGuys
    {
        public string Name {get;}
        public int Health {get; protected set;}
        public int Damage {get; protected set;}
        public int Defense {get; protected set;}
        public int VictoryPoints {get; protected set;}

        public BadGuys (string name)
        {
            this.Name = name;
            this.Health = 100;
            this.Damage = 10;
            this.Defense = 100;
        }  
    }
}