using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Entidades
{
    public class Calculadora
    {
        public static double Operar(Numero num1, Numero num2, string operador)
        {
            double total;

            operador = ValidarOperador(operador);

            switch(operador)
            {
                case "-":
                    {
                        total = num1 - num2;
                        break;
                    }
                case "*":
                    {
                        total = num1 * num2;
                        break;
                    }
                case "/":
                    {
                        total = num1 / num2;
                        break;
                    }
                default:
                    {
                        total = num1 + num2;
                        break;
                    }
            }
            return total;
        }

        /// <summary>
        /// Valida el ingreso de los operadores comunes, estableciendo a "+" como la variable default
        /// </summary>
        /// <param name="operador"></param>
        /// <returns>Retorno</returns>
        private static string ValidarOperador(string operador)
        {

            string retorno = "";
            switch (operador)
            {
                case "-":
                    {
                        retorno = "-";
                        break;
                    }
                case "*":
                    {
                        retorno = "*";
                        break;
                    }
                case "/":
                    {
                        retorno = "/";
                        break;
                    }
                default:
                    {
                        retorno = "+";
                        break;
                    }
                    
            }
            return retorno;
        }
    }
}