using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WindowsFormsApp1
{
    internal class Banco : IValidar
    {
        private List<Cliente> clientes;
        private string nombre;

        public Banco(string nombre_ingresado)
        {
            nombre = nombre_ingresado;

            using (StreamReader archivo = new StreamReader(@"D:\Drive\UTN\_Programacion 3\Práctica\Proyectos VS\TP3_Parte_2\bancos\" + nombre + ".txt"))
            {
                int linea = 0;
                string datos_cliente = "";
                while ((datos_cliente = archivo.ReadLine()) != null)
                {
                    string[] campos = datos_cliente.Split('|');
                    /*
                    Cliente nuevoCliente = new Cliente(campos[0], campos[1], campos[2], decimal.Parse(campos[3]));
                    // Si el arreglo esta lleno...
                    if (linea == clientes.Length)
                    {
                        Array.Resize(ref clientes, linea + 1);
                    }
                    clientes[linea] = nuevoCliente;
                    */
                    linea++;
                }

            }
        }

        public bool AgregarCliente()
        {
            return true;
        }

        private Cliente BuscarCliente()
        {
            return null;
        }

        public decimal SaldoTesoreria()
        {
            return 10;
        }

        public void OperarCliente()
        {

        }

        public void Cerrar()
        {

        }

        public bool ValidarString(string stringValidable)
        {
            throw new NotImplementedException();
        }

        public bool ValidarCuit(string cuitValidable)
        {
            throw new NotImplementedException();
        }
        /*
nombre = nombre_ingresado;

using (StreamReader archivo = new StreamReader(@"D:\Drive\UTN\_Programacion 3\Práctica\Proyectos VS\TP3\" + nombre + ".txt"))
{
   int linea = 0;
   string datos_cliente = "";
   while ((datos_cliente = archivo.ReadLine()) != null)
   {
       string[] campos = datos_cliente.Split('|');

       Cliente nuevoCliente = new Cliente(campos[0], campos[1], campos[2], decimal.Parse(campos[3]));
       // Si el arreglo esta lleno...
       if (linea == clientes.Length)
       {
           Array.Resize(ref clientes, linea + 1);
       }
       clientes[linea] = nuevoCliente;

       linea++;
   }

}

public bool AgregarCliente(string nombre, string apellido, string cuit)
{

// Controla si un cuit existe o no detro de la cartera de clientes
bool NuevoCuitEsUnico(string nuevo_cuit)
{
   bool result = true;
   foreach (Cliente cliente in clientes)
   {
       if (cliente.Cuit == nuevo_cuit)
       {
           result = false;
       }
   }
   return result;
}

// Validar la entrada
// @nombre, @apellido string sin numero ni caracteres especiales
// @cuit string de 11 numeros sin símbolos

bool error = false;

if (!ValidarString(nombre))
{
   Console.WriteLine("Nombre inválido");
   error = true;
}
else if (!ValidarString(apellido))
{
   Console.WriteLine("Apellido inválido");
   error = true;
}
else if (!ValidarCuit(cuit))
{
   Console.WriteLine("CUIT inválido");
   error = true;
}
else if (!NuevoCuitEsUnico(cuit))
{
   Console.WriteLine("El CUIT ingresado ya pertenece a un usuario");
   error = true;
}
// Si no hay error...
if (!error)
{
   //Instancia un cliente
   Cliente nuevoCliente = new Cliente(nombre, apellido, cuit);
   // Agranda el arreglo
   Array.Resize(ref clientes, clientes.Length + 1);
   // Inserta el cliente en la una posicion vacía del arreglo
   clientes[clientes.Length - 1] = nuevoCliente;
   // Informa estado actual del banco
   Console.WriteLine("\nCantidad actualizada de clientes del banco: " + clientes.Length + " clientes.");
   Console.WriteLine("Saldo en Tesorería: $" + SaldoActualTesoreria());
   return true;
}
else
{
   return false;
}

}

public decimal SaldoActualTesoreria()
{
decimal saldo = 0;
foreach (Cliente cliente in clientes)
{
   if (cliente != null)
   {
       saldo += cliente.Saldo();
   }
   else
   {
       break;
   }
}
return saldo;
}

public Cliente OperarCliente(string cuitBuscado)
{
foreach (Cliente cliente in clientes)
{
   if (cliente.Cuit == cuitBuscado)
   {
       return cliente;
   }
}
return null;
}

public void Cerrar()
{
Console.Clear();
Console.WriteLine("Saliendo...\n\nSaldo total en tesorería del banco: " + SaldoActualTesoreria());
Console.ReadLine();
void PersistirClientes()
{
   Console.WriteLine("\nPersisitiendo listado de clientes del banco.");
   string[] lineas_clientes = new string[clientes.Length];
   for (int i = 0; i < clientes.Length; i++)
   {
       lineas_clientes[i] = clientes[i].Nombre + "|" + clientes[i].Apellido + "|" + clientes[i].Cuit + "|" + clientes[i].Monto.ToString();
       File.WriteAllLines(@"D:\Drive\UTN\_Programacion 3\Práctica\Proyectos VS\TP3\" + nombre + ".txt", lineas_clientes);
   }
}
PersistirClientes();
}

public bool ValidarString(string texto)
{
return Regex.IsMatch(texto, @"^[a-zA-Z]+$");
}

public bool ValidarCuit(string cuit)
{
if (string.IsNullOrEmpty(cuit)) throw new ArgumentNullException(nameof(cuit));
if (cuit.Length != 13) throw new ArgumentException(nameof(cuit));
bool rv = false;
int verificador;
int resultado = 0;
string cuit_nro = cuit.Replace("-", string.Empty);
string codes = "6789456789";
long cuit_long = 0;
if (long.TryParse(cuit_nro, out cuit_long))
{
   verificador = int.Parse(cuit_nro[cuit_nro.Length - 1].ToString());
   int x = 0;
   while (x < 10)
   {
       int digitoValidador = int.Parse(codes.Substring((x), 1));
       int digito = int.Parse(cuit_nro.Substring((x), 1));
       int digitoValidacion = digitoValidador * digito;
       resultado += digitoValidacion;
       x++;
   }
   resultado = resultado % 11;
   rv = (resultado == verificador);
}
return rv;

}
*/
    }
}