public class Books {
    private static int nextId = 0;
    private int ID;
    private String Name;
    private String Author;
    private String Year;
    private int Amount;

    public Books() {
        ID = 0;
        Name="";
        Author="";
        Year="";
        Amount=0;
    }

    public Books( int newID, String newName, String newAuthor, String Year, String newYear, int newAmount)
    {
        setID( newID);
        setName(newName);
        setAuthor(newAuthor);
        setYear(newYear);
        setAmount(newAmount);
    }

    protected void setID( int newID)
    {
        this.ID = newID;
    }

    public int getID() {
        return this.ID;
    }

    protected void setName(String newName)
    {
        this.Name = newName;
    }

    public String getName()
    {
        return this.Name;
    }

    protected void setAuthor(String newAuthor)
    {
        this.Author = newAuthor;
    }

    public String getAuthor() {
        return this.Author;
    }

    protected void setYear(String newYear)
    {
        this.Year = newYear;
    }

    public String getYear() {
        return this.Year;
    }

    protected void setAmount(int newAmount)
    {
        this.Amount = newAmount;
    }

    public int getAmount() {
        return this.Amount;
    }


}
