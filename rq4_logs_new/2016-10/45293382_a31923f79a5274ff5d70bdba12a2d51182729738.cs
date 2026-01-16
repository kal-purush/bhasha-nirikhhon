using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Drawing;
using System.Drawing.Imaging;

namespace SmoothingFilter
{
    class Program
    {
        static void Main(string[] args)
        {
            const int size = 256;
            double[] data = new double[size];
            double[] re;								// 実数
            double[] im = new double[size];	// 虚数
            // 適当なデータを用意
            for (int i = 0; i < size; i++)
            {
                data[i] = i;
            }
            // 高速フーリエ変換(データが256個なのでbitSizeは8)
            FFT(data, im, out re, out im, 8);
            // 出力
            for (int i = 0; i < size; i++)
            {
                Console.WriteLine("{0} {1} {2}", data[i], re[i], im[i]);
            }
        }
        // 高速フーリエ変換
        public static void FFT(double[] re1, double[] im1, out double[] re2, out double[] im2, int bitSize)
        {
            int dataSize = 1 << bitSize;
            int[] reverseBitArray = BitScrollArray(dataSize);

            re2 = new double[dataSize];
            im2 = new double[dataSize];

            // バタフライ演算のための置き換え
            for (int i = 0; i < dataSize; i++)
            {
                re2[i] = re1[reverseBitArray[i]];
                im2[i] = im1[reverseBitArray[i]];
            }

            // バタフライ演算
            for (int stage = 1; stage <= bitSize; stage++)
            {
                int butterflyDistance = 1 << stage;
                int numType = butterflyDistance >> 1;
                int butterflySize = butterflyDistance >> 1;
                double wRe = 1.0;
                double wIm = 0.0;
                double uRe = System.Math.Cos(System.Math.PI / butterflySize);
                double uIm = -System.Math.Sin(System.Math.PI / butterflySize);

                for (int type = 0; type < numType; type++)
                {
                    for (int j = type; j < dataSize; j += butterflyDistance)
                    {
                        int jp = j + butterflySize;
                        double reTemp = re2[jp] * wRe - im2[jp] * wIm;
                        double imTemp = re2[jp] * wIm + im2[jp] * wRe;
                        re2[jp] = re2[j] - reTemp;
                        im2[jp] = im2[j] - imTemp;
                        re2[j] += reTemp;
                        im2[j] += imTemp;
                    }
                    double tempWRe = wRe * uRe - wIm * uIm;
                    double tempWIm = wRe * uIm + wIm * uRe;
                    wRe = tempWRe;
                    wIm = tempWIm;
                }
            }
        }

        // ビットを左右反転した配列を返す
        private static int[] BitScrollArray(int arraySize)
        {
            int[] reBitArray = new int[arraySize];
            int arraySizeHarf = arraySize >> 1;

            reBitArray[0] = 0;
            for (int i = 1; i < arraySize; i <<= 1)
            {
                for (int j = 0; j < i; j++)
                    reBitArray[j + i] = reBitArray[j] + arraySizeHarf;
                arraySizeHarf >>= 1;
            }
            return reBitArray;
        }

    }
}