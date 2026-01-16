Console.Clear();
Random random = new Random();
int num = random.Next(1, 101);
int tentativas = 0;

blue();
Console.WriteLine("Jogo de Adivinhação");
reset();

red();
Console.WriteLine("\nVocê somente tera 7 tentativas!");
reset();

blue();
Console.WriteLine("\nTente adivinhar o número secreto entre 1 e 100.");
reset();

while (true)
{
    yellow();
    Console.Write("\nDigite o seu palpite: ");
    reset();
    int chute = int.Parse(Console.ReadLine()!);
    tentativas++;

        if (chute == num)
        {
            yellow ();
            Console.WriteLine($"\nParabéns! Você acertou o número é {num}!!!");
            reset();
            break;
        }

        if (tentativas == 7)
        {
            red();
            Console.WriteLine($"Suas tentativas acabaram! O número era {num}. Você perdeu o jogo.");
            reset();
            break;
        }

        int dif = Math.Abs(chute - num);

        if (dif <= 3)
        {
            red();
            Console.WriteLine("Está pelando!");
        }   
        else if (dif <= 10)
        {
            red();
            Console.WriteLine("Está quente!");
            reset();
        }
        else if (dif >= 30)
        {
            blue();
            Console.WriteLine("Está congelando!");
        }
        else
        {
            blue();
            Console.WriteLine("Está frio!");
            reset();
        }

        if (chute < num)
        {
            blue();
            Console.WriteLine("\nO número é maior.");
            reset();
        }
        if (chute > num)
        {
            blue();
            Console.WriteLine("\nO número é menor.");
            reset();
        }
        {
    }
}  

void reset()
{
    Console.ResetColor();
}

void blue()
{
    Console.ForegroundColor = ConsoleColor.Blue;
}

void red()
{
    Console.ForegroundColor = ConsoleColor.Red;
}

void yellow()
{
    Console.ForegroundColor = ConsoleColor.Yellow;
}