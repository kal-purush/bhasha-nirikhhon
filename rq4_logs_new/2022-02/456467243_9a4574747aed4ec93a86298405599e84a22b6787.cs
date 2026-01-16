using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AtlagHom {
    internal class Program {
        static void Main(string[] args) {
            int[,] data = new int[12,30];
            Random random = new Random();

            //random adatok
            for (int i = 0; i < data.GetLength(0); i++) {
                for (int j = 0; j < data.GetLength(1); j++) {
                    data[i, j] = random.Next(-10, 40);
                    Console.Write("{0} ",data[i,j]);
                }
                Console.WriteLine();
            }
            Console.WriteLine();

            int min = Int32.MaxValue;
            int min_i = -1;
            int min_j = -1;
            int max = Int32.MinValue;
            int max_i = -1;
            int max_j = -1;
            int honap_hom_sum = 0;
            float[] honap_hom = new float[12];
            int index = -1;
            bool day = false;
            int day_count = 0;

            for (int i = 0; i < data.GetLength(0); i++) {
                for (int j = 0; j < data.GetLength(1); j++) {
                    //min es max homerseklet keresese
                    if (data[i, j] < min) {
                        min = data[i, j];
                        min_i = i;
                        min_j = j;
                    }
                    if(data[i, j] > max) {
                        max = data[i, j];
                        max_i = i;
                        max_j = j;
                    }  
                    //akt honap hom sum
                    honap_hom_sum += data[i, j];

                    //ot nap minusz
                    if (!day) {
                        if (data[i, j] < 0) {
                            day_count++;
                        } else {
                            day_count = 0;
                        }
                        if (day_count == 5) {
                            day = true;
                        }
                    }
                }
                //honapok atlag homerseklete
                honap_hom[++index] = honap_hom_sum / 30;
                honap_hom_sum = 0; //akt honap nullazasa
            }

            //ot minusz egymas utan
            if (day) {
                Console.WriteLine("Volt egymas utan ot nap minusz fok");
            } else {
                Console.WriteLine("Nem volt egymas utan ot nap minusz fok");
            }

            Console.WriteLine("Az ev legmelegebb napja: {0}, {1}", max_i, max_j);
            Console.WriteLine("Az ev leghidegebb napja: {0}, {1}", min_i, min_j);
            Console.WriteLine();

            float min_honap = 100;
            float max_honap = -100;

            for (int i = 0; i < honap_hom.Length; i++) {
                Console.Write("{0} ",honap_hom[i]);
            }
            
            //min es max honap atlag hom keresese
            for (int i = 0; i < honap_hom.Length; i++) {
                if(min_honap > honap_hom[i]) {
                    min_i = i;
                    min_honap = honap_hom[i];
                }
                if(max_honap < honap_hom[i]) {
                    max_i = i;
                    max_honap = honap_hom[i];
                }
            }

            Console.WriteLine("\nLegmelegebb honap: {0}", (max_i+1));
            Console.WriteLine("Leghidegebb honap: {0}", (min_i+1));

            Console.ReadKey();
        }
    }
}