using System;

namespace Alg_3._0
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Podaj a: ");
            float a = float.Parse(Console.ReadLine());
            
            Console.WriteLine("Podaj b: ");
            float b = float.Parse(Console.ReadLine());
           
            Console.WriteLine("Podaj c: ");
            float c = float.Parse(Console.ReadLine());
            
            Console.WriteLine("Podaj d: ");
            float d = float.Parse(Console.ReadLine());
            


            Console.WriteLine("Podane liczby to: " + a + ", " + b + ", " +c+ ", " +d );
            

            //w
            double  w = -b / (3 * a);
           // Console.WriteLine("w: " +  w);

            //p
            double p = ((3 * a * Math.Pow(w, 2) + 2 * b * w + c) / a);
           // Console.WriteLine("p: "  +p);

            //q
            double q = (((a * Math.Pow(w, 3)) + (b * Math.Pow(w, 2)) + (c * w) + d) / a);
           // Console.WriteLine("q: " + q);

            //Delta
            double delta = (Math.Pow(q, 2) / 4) + (Math.Pow(p, 3) / 27);
            Console.WriteLine("delta: " + delta);

            if (delta > 0)
            {
                Console.WriteLine("Przypadek 1");
               double p1 = -q / 2;
               double p2 = Math.Pow(delta, 1D / 2);
               double p3 = (p1 -p2);

             
               // Console.WriteLine("p1: "  +p1);
               // Console.WriteLine("p2: " +p2);
               // Console.WriteLine("p3: " +p3);

                //u
                double u = Math.Pow(p1 + p2, 1D / 3);
                Console.WriteLine("u: " +  u);

            
                //v
                if (p3 < 0)
                {
                    double v = Math.Pow((p3 * -1),1D /3) * -1;
                    Console.WriteLine("v: " +v);


                    //x1
                    double x1 = u + v + w;

                    //x2
                    double x2 = -1 / 2 * (u + v) + w + (Math.Pow(3, 1D / 2) / 2) * (u - v);

                    //x3
                    double x3 = -1 / 2 * (u + v) + w - (Math.Pow(3, 1D / 2) / 2) * (u - v);


                    Console.WriteLine("x1: "  +x1);
                    Console.WriteLine("x2: "  +x2 + "i");
                    Console.WriteLine("x3: "  +x3 + "i");
                }
                else
                {
                    double v = Math.Pow((p3), 1D / 3);
                    Console.WriteLine("v: " + v);

                    //x1
                    double x1 = u + v + w;
                    //x2
                    double x2 = -1 / 2 * (u + v) + w + (Math.Pow(3, 1D / 2) / 2) * (u - v);
                    //x3
                    double x3 = -1 / 2 * (u + v) + w - (Math.Pow(3, 1D / 2) / 2) * (u - v);

                    Console.WriteLine("x1: " +x1);
                    Console.WriteLine("x2: " +x2 + "i");
                    Console.WriteLine("x3: " +x3 + "i");
                }
               
                

            }
            else if (delta < 0)
            {
                Console.WriteLine("Przypadek 2");
                double o = Math.Acos(3 * q / (2 * p * ((Math.Pow(-p / 3, 1D / 2)))));

                //x1
                double x1 = w + 2 * Math.Pow(-p / 3, 1D / 2) * Math.Cos(o / 3);

                //x2
                double x2 = w + 2 * Math.Pow(-p / 3, 1D / 2) * Math.Cos(o / 3 + (2 / 3) * 3.14);

                //x3
                double x3 = w + 2 * Math.Pow(-p / 3, 1D / 2) * Math.Cos(o / 3 + (4 / 3) * 3.14);

                Console.WriteLine("x1: " + x1);
                Console.WriteLine("x2: " + x2);
                Console.WriteLine("x3: " + x3);

            }
            else
            {
                Console.WriteLine("Przypadek 3");
                //x1
                double x1 = w - 2 * Math.Pow(q / 2, 1D / 3);

                //x2,x3
                double x2 = w + Math.Pow(q / 2, 1D / 3);

                Console.WriteLine("x1: " + x1);
                Console.WriteLine("x2,x3: " + x2);

            }


        }
    }
}