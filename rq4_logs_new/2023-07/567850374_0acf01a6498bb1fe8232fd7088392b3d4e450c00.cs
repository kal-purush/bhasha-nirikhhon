using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Entities
{
    internal class Contract
    {
        public int NumeroContrato { get; private set; }
        public DateOnly Data { get; private set; }
        public double ValorTotal { get; private set; }
        public List<Installment> Installment;

        public Contract(int numero, DateOnly data, double valorTotal)
        {
            NumeroContrato = numero;
            Data = data;
            ValorTotal = valorTotal;
            Installment = new List<Installment>();
        }

        public void AdicionarParcela(Installment installment)
        {
            Installment.Add(installment);
        }

        public void RetornarLista()
        {
            foreach (Installment installment in Installment)
            {
                Console.WriteLine(installment.Quantia);
            }
        }

        //public override string ToString()
        //{
        //    return ;
        //}
    }
}