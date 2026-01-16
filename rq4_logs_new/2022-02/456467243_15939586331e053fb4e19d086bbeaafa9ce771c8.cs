using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Szamkitalalo {
    class Program {
        static void Main(string[] args) {
            Random random = new Random();
            short pc_random = (short)random.Next(1, 100);
            short user_random = 0;
            short tipp_counter = 0;
            bool isFelhasznalo = false;

            Console.WriteLine("Játékos vagy gép tippel? Ha jatekos irj 1-et");
            short valasztas = Convert.ToInt16(Console.ReadLine());
            if(valasztas == 1) {
                isFelhasznalo = true;
            }

            while (tipp_counter < 5) {
                if (isFelhasznalo) {
                    while (tipp_counter < 5) {
                        tipp_counter++;
                        Console.WriteLine("Tippelj (1-100)");
                        short user_tipp = Convert.ToInt16(Console.ReadLine());

                        if (pc_random > user_tipp) {
                            Console.WriteLine("Tul kicsi a tipp");
                        } else if (pc_random < user_tipp) {
                            Console.WriteLine("Tul nagy a tipp");
                        } else if (pc_random == user_tipp) {
                            Console.WriteLine("Nyertel");
                            Console.WriteLine("PC tippje: {0}", pc_random);
                            break;
                        }
                        if(tipp_counter == 5) {
                            Console.WriteLine("PC tippje: {0}",pc_random);
                        }
                    }
                } else {
                    Console.WriteLine("Irj be egy szamot (1-100)");
                    user_random = Convert.ToInt16(Console.ReadLine());
                    short rand_min = 1;
                    short rand_max = 100;

                    while (user_random != pc_random) {
                        tipp_counter++;
                        pc_random = (short)random.Next(rand_min, rand_max);
                        Console.WriteLine("PC tipp {0}", pc_random);
                        if (pc_random < user_random) {
                            Console.WriteLine("Tul kicsi a tipp");
                            rand_min += 10;
                            if(rand_min > user_random) {
                                rand_min = user_random;
                            }
                        } else if (pc_random > user_random) {
                            Console.WriteLine("Tul nagy a tipp");
                            rand_max -= 10;
                            if(rand_max < user_random) {
                                rand_max = user_random;
                            }
                        } else if (pc_random == user_random) {
                            Console.WriteLine("PC nyert");
                            Console.WriteLine("Felhasznalo tippje: {0}", user_random);
                            break;
                        }
                    }
                }        
            }

            Console.ReadKey();
        }
    }
}