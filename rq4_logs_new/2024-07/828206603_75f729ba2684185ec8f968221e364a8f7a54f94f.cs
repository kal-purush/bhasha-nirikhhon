class LibraryCount
{ 
    private int startedDeposit = 10000; // the amount of books when Library opened

    public int GetStartedDeposit()
    {
        return startedDeposit;
    }

    public int Remove(int a, int startedDeposit) // call method if reader's reply has "take"
    {
        int result = startedDeposit - a;
        return result;
    }

    public int Add(int a, int startedDeposit) // call method if reader's reply has "back"
    {
        int result = startedDeposit + a;
        return result;
    }
}
