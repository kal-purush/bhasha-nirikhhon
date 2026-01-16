using System;
using System.Globalization;

public class Program
{
    private static int horas;

    private static void Main(string[] args)
    {
        Console.WriteLine("##\n Estrutura Condicional! ##");

        int = horas;

        Console.WriteLine("Quantas horas? ");
        horas = int.Parse(Console.ReadLine());

        if (horas < 12)
        {
            Console.WriteLine("Bom dia!");
        }
        else
        {
            if (horas < 18)
            {
                Console.WriteLine("Boa Tarde!");
            }
            else
            {
                Console.WriteLine("Boa noite!");
            }
        }

        Console.WriteLine("-----------------------");

        Console.WriteLine(" ##\n Exercicios Resolvidos ##");
        double nota1, nota2, soma;
        nota1 = double.Parse(Console.ReadLine(), CultureInfo.InvariantCulture);
        nota2 = double.Parse(Console.ReadLine(), CultureInfo.InvariantCulture);

        soma = nota1 + nota2;

        Console.WriteLine("NOTA FINAL = " + soma.ToString("F1", CultureInfo.InvariantCulture));

        if (soma < 60.0)
        {
            Console.WriteLine("REPROVADO");
        }

        Console.WriteLine("------------------------");

        // Declaração da constante para π
        const double pi = 3.14159;

        // Leitura do valor do raio
        double raio = double.Parse(Console.ReadLine());

        // Cálculo da área
        double area = pi * Math.Pow(raio, 2);

        // Impressão do resultado com 4 casas decimais
        Console.WriteLine("A=" + area.ToString("F4"));

        Console.WriteLine("------------------------");

        double a, b, c, delta, r1, r2;

        string[] vet = Console.ReadLine().Split(' ');

        a = double.Parse(vet[0], CultureInfo.InvariantCulture);
        b = double.Parse(vet[1], CultureInfo.InvariantCulture);
        c = double.Parse(vet[2], CultureInfo.InvariantCulture);

        delta = Math.Pow(b, 2.0) - 4 * a * c;

        if (a == 0 || delta < 0.0)
        {
            Console.WriteLine("Impossivel caulcular");
        }
        else
        {
            r1 = (-b + Math.Sqrt(delta)) / (2.0 * a);
            r2 = (-b - Math.Sqrt(delta)) / (2.0 * a);
            Console.WriteLine("R1 = " + r1.ToString("F5", CultureInfo.InvariantCulture));
            Console.WriteLine("R2 = " + r2.ToString("F5", CultureInfo.InvariantCulture));
        }
    }
}