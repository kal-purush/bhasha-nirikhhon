using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MeuProjeto.Tests
{
    public class Calculadora
    {
        private List<string> historico = new List<string>();

        public int Soma(int a, int b)
        {
            int resultado = a + b;
            historico.Add($"Soma: {a} + {b} = {resultado}");
            return resultado;
        }

        public int Subtracao(int a, int b)
        {
            int resultado = a - b;
            historico.Add($"Subtracao: {a} - {b} = {resultado}");
            return resultado;
        }

        public int Multiplicacao(int a, int b)
        {
            int resultado = a * b;
            historico.Add($"Multiplicacao: {a} * {b} = {resultado}");
            return resultado;
        }

        // Método para obter o histórico de operações
        public List<string> ObterHistorico()
        {
            return historico;
        }
    }

}