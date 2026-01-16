class LibraryCount
{ 
    public void MesAdd(int a, int result) // message if reader takes a book
    {
        Console.WriteLine($"Thank You. You take {a} book(s), and our depositary for now is {result} items!");
        Console.WriteLine("Have a good time! See you back soon!");
    }

    public void MesRemove(int b, int result) // message if reader gives a book
    {
        Console.WriteLine($"Thank You. You give us back {b} book(s), and our depositary for now is {result} items!");
        Console.WriteLine("See you back soon!");
    }

    public void Add(int a, int _StartedDeposit) // call method if reader's reply has "take"
    {
        int result = _StartedDeposit - a;
        MesAdd(a, result);
    }

    public void Remove(int b, int _StartedDeposit) // call method if reader's reply has "back"
    {
        int result = _StartedDeposit + b;
        MesRemove(b, result);
    }
}
