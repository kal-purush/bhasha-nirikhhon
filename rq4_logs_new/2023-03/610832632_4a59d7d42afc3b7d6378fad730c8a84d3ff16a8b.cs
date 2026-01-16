// 2. Напишите программу, которая принимает на вход координаты
// двух точек и находит расстояние между ними в 2D пространстве.

Console.Write("x1=");
int x1=int.Parse(Console.ReadLine()!);

Console.Write("y1=");
int y1=int.Parse(Console.ReadLine()!);

Console.Write("x2=");
int x2=int.Parse(Console.ReadLine()!);

Console.Write("y2=");
int y2=int.Parse(Console.ReadLine()!);

double Length (int x1, int y1, int x2, int y2){

return Math.Round(Math.Sqrt(Math.Pow(x2-x1,2)+Math.Pow(y2-y1,2)),3);
}

Console.Write(Length(x1,y1,x2,y2) );