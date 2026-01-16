class Databas
{
    public int id { get; set; }
    public string name { get; set; }
    public string eMail { get; set; }
    public int phoneNumber { get; set; }
    public string city { get; set; }
    public int postalCode { get; set; }
    public string title { get; set; }
    public int Year { get; set; }
    public int minutes { get; set; }
    public int serialNumber { get; set; }
    public int price { get; set; }
    public string director { get; set; }

    public override string ToString()
    {
        return $"{id} {name}";
    }

}