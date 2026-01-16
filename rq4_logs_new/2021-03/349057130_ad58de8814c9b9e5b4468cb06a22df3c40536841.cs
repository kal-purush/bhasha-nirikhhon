using System;

namespace Practica_06
{

    // PRACTICA 1:
    /*public class Persona{
        protected string cedula;
        protected string nombre, apellido;
        protected int edad;
        public string Cedula { get{return cedula;}set{cedula = value;}}
        public string Nombre { get{return nombre;}set{nombre = value;}}
        public string Apellido { get{return apellido;}set{apellido = value;}}
        public int Edad { get{return edad;}set{edad = value;}}
        public Persona(string cedula, string nombre, string apellido, int edad){
            this.cedula = cedula;
            this.nombre = nombre;
            this.apellido = apellido;
            this.edad = edad;
        }
        public virtual void Mostrar(){
            Console.WriteLine("Su cedula es: {0}", cedula);
            Console.WriteLine("Su nombre es: {0}", nombre);
            Console.WriteLine("Su apellido es: {0}", apellido);
            Console.WriteLine("Su edad es: {0}", edad);
        }
    }
   public class Profesor : Persona{
       int sueldo;
       public Profesor(string cedula, string nombre, string apellido, int edad, int sueldo):base(cedula, nombre, apellido, edad){
           this.sueldo = sueldo;
       }
        public int Sueldo {get{return sueldo;}set{sueldo = value;}}
    public override void Mostrar(){
        Console.WriteLine("El sueldo es: {0}", sueldo);
    }

   }
    class Program
    {
        static void Main(string[] args)
        {
            Persona p = new Persona("", "","", 0);
            Profesor pro = new Profesor("", "","", 0, 0);
            p.Cedula = "001-402-30432-8";
            p.Nombre = "Mateo";
            p.Apellido = "Gonzales";
            p.Edad = 32;
            pro.Sueldo = 30000;
            p.Mostrar();
            pro.Mostrar();
        }
    }
}*/


//PRACTICA 2:
/*public class Contacto{
    string nombre, apellido, telefono, direccion;
    public void SetContacto(string nombre, string apellido, string telefono, string direccion){
        this.nombre = Console.ReadLine();
        this.apellido = Console.ReadLine();
        this.telefono = Console.ReadLine();
        this.direccion = Console.ReadLine();
    }
    public void Saludar(){
        Console.WriteLine("Hola soy {0} {1}, mi telefono es {2} y vivo en {3}", nombre, apellido, telefono, direccion);
        Console.WriteLine();
    }
}
public class ProbarContacto{
class Program
    {
        static void Main(string[] args)
        {
            Contacto c = new Contacto();
            Contacto o = new Contacto();
            Console.WriteLine("Ingrese valores: ");
            c.SetContacto("","","","");
            c.Saludar();

            Console.WriteLine("Ingrese valores: ");
            o.SetContacto("","","","");
            o.Saludar();
        }
    }
}
}*/


//PRACTICA 3:
/*public class ClaseA{
    public ClaseA(){
        Console.WriteLine("Que bello es vivir");
    }
}
public class ClaseB : ClaseA{
    public ClaseB(){
        Console.WriteLine("La vida es corta aprovechala");
    }
}
public class ClaseC : ClaseB{
    public ClaseC(){
        Console.WriteLine("Aprovechemos el tiempo");
    }
}
class Program
    {
        static void Main(string[] args)
        {
            ClaseC c = new ClaseC();
        }
    }
}*/