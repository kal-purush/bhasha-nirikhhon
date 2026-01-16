namespace API.DTO;

public class MembersDto
{
    public int Id { get; init; }
    
    public string? Username { get; set; }
    
    public string? PhotoUrl { get; set; }
    
    public int Age { get; init; }
    
    public string? KnownAs { get; set; }

    public DateTime Created { get; init; }

    public DateTime LastActivity { get; set; } 
    
    public string? Gender { get; init; }
    
    public string? Introduction { get; set; }
    
    public string? Interests { get; set; }

    public string? LookingFor { get; set; }

    public  string? City { get; init; }

    public string? Country { get; init; }

    public List<PhotoDto>? Photos { get; set; }
}