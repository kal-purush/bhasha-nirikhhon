namespace QRisto.Persistence.Entity.Provider;

public class AddressEntity : IEntity
{
    public Guid Id { get; set; }
    
    public DateTime CreatedDate { get; set; }
    
    public DateTime ModificationDate { get; set; }
    
    public string Name { get; set; }
    
    public string Country { get; set; }
    
    public string State { get; set; }
    
    public string City { get; set; }
    
    public string Street1 { get; set; }
    
    public string Street2 { get; set; }
    
    public string Zip { get; set; }
    
    public string Building { get; set; }
    
    public string Office { get; set; }
}