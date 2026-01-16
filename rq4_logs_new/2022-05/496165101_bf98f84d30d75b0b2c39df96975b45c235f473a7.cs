// See https://aka.ms/new-console-template for more information


string data = System.IO.File.ReadAllText("data.txt");
int floor = 0;
int cnt = 0;
int idx = -1;
foreach (var c in data)
{
    cnt += 1;
    if (c == '(')
    {
        floor += 1;
    }
    else
    {
        floor -= 1;
    }
    if (idx == -1 && floor == -1)
    {
        idx = cnt ;
        
    }

}
Console.WriteLine("Part 1: {0}", floor);
Console.WriteLine("Part 2: {0}", idx);