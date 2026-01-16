using MathNet.Numerics.IntegralTransforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace Teste2Fourier
{
    public class FrequenciaDolar
    {

        public Complex[,] ObterFrequencia (double[,] somaDeOndas)
        {
            Complex[,] complex = new Complex[9, 30];
            for (int coluna = 0; coluna < 9; coluna++)
            {
                Complex[] complex1 = new Complex[30];
                for (int linha = 0; linha < 30; linha++)
                {
                    complex[coluna, linha] = new Complex(somaDeOndas[coluna, linha], 0);
                    complex1[linha] = complex[coluna, linha];
                }

                Fourier.Forward(complex1, FourierOptions.NoScaling);

                for (int linha2 = 0; linha2 < 30; linha2++)
                {
                    complex[coluna, linha2] = complex1[linha2];
                }
            }
            return complex;
        }
    }
}