class Library
{
    // constructor for initialilizing
    public Library()
    {
        Deposit = _startDeposit;
        ListOfBooks = new List<Book>();
    }

    // Deposit
    private int _startDeposit = 10000;

    public int Deposit { get; private set; }

    // List of Books
    public List<Book> ListOfBooks {get; private set; }

    // Adding book's info to Library
    public void AddBooksInfo(Book book)
    {
        ListOfBooks.Add(book);
    }

    // DisplayReceivedOrTakenBooks()
    public void DisplayBooks()
    {
        foreach (var book in ListOfBooks)
        {
            book.DisplayBookInfo();
        }
    }

}