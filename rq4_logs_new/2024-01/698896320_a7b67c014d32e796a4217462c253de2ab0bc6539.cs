using static System.Net.Mime.MediaTypeNames;
using System.Globalization;

Console.WriteLine(" ##\n Exercicios ## ");


Console.WriteLine("Entre com o seu nome completo: ");

String fullName = Console.ReadLine();

Console.WriteLine("Quantos quartos tem na sua casa ? ");

int bedrooms = int.Parse(Console.ReadLine());



Console.WriteLine("Entre com o preço de um produto:" );

Double price = (double.Parse(Console.ReadLine(), CultureInfo.InvariantCulture);



Console.WriteLine("Entre seu ultimo nome, idade e altura");



String[] vet = Console.ReadLine().Split(' ');

String lastName = vet[0];

int age = int.Parse(vet[1]);

Double height = double.Parse(vet[2], CultureInfo.InvariantCulture);



Console.WriteLine(fullName);

Console.WriteLine(bedrooms);

Console.WriteLine(price.ToString("F2", CultureInfo.InvariantCulture));

Console.WriteLine("lastname");

Console.WriteLine(age);

Console.WriteLine(height.ToString("F2", CultureInfo.InvariantCulture));