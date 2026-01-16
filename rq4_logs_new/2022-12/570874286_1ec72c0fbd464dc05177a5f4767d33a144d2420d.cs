int ReadNumber(string message){
    Console.Write(message);
    var s = Console.ReadLine();
    int a = s == null? 0 : int.Parse(s);
    return a;
}

double Distance(int ax, int ay, int az,int bx,int by,int bz){
    return Math.Sqrt(Math.Pow(bx-ax,2)+Math.Pow(by-ay,2)+Math.Pow(bz-az,2));
}

int ax = ReadNumber("Введите aX:");
int ay = ReadNumber("Введите aY:");
int az = ReadNumber("Введите aZ:");
int bx = ReadNumber("Введите bX:");
int by = ReadNumber("Введите bY:");
int bz = ReadNumber("Введите bZ:");

Console.WriteLine(Math.Round(Distance(ax,ay,az,bx,by,bz),2));