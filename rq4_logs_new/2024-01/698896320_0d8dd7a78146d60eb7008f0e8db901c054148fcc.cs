using System.Globalization;

Console.WriteLine("Entrada de Dados 2 ");

int n1 = int.Parse(Console.ReadLine()); //Declarando variável n1 do tipo inteira, forçando sua impressão com PARSE pois console.readline so imprime do tipo STRING
char ch = char.Parse(Console.ReadLine()); //Declarando variável ch do tipo char, forçando sua impressão com PARSE por  console.readline so imprime do tipo STRING
double n2 = double.Parse(Console.ReadLine(), CultureInfo.InvariantCulture); //Declarando variável n2 do tipo double, forçando sua impressão com PARSE por  console.readline so imprime do tipo STRING 

string[] vet = Console.ReadLine().Split(' ');
string nome = vet[0];
char sexo = char.Parse(vet[1]);
int idade = int.Parse(vet[2]);
double altura = double.Parse(vet[3], CultureInfo.InvariantCulture);

Console.WriteLine("Você digitou: ");
Console.WriteLine(n1);
Console.WriteLine(ch);
Console.WriteLine(n2);
Console.WriteLine(nome);
Console.WriteLine(sexo);
Console.WriteLine(idade);
Console.WriteLine(altura);