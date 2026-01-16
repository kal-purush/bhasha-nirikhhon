using System;
using System.Collections.Generic;
using System.Text;

namespace Calculadora
{
    class Calculadora
    {
        public static double DoOperation(double n1, double n2, string op)
        {
            double res = double.NaN; // O valor padrão é "não um número" que usamos se uma operação, como divisão, puder resultar em um erro.
            
            // Use uma instrução switch para fazer as contas.
            switch(op)
            {
                case "a":
                    res = n1 + n2;
                    break;
                case "s":
                    res = n1 - n2;
                    break;
                case "m":
                    res = n1 * n2;
                    break;
                case "d":
                    // Peça ao usuário para inserir um divisor diferente de zero.
                    if(n2 != 0)
                    {
                        res = n1 / n2;
                    }
                    break;
                // Retorna o texto para uma entrada de opção incorreta.
                default:
                    break;
            }
            return res;
        }
    }
}