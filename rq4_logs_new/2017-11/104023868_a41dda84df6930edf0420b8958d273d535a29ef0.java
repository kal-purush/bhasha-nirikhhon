
public class Athlete {
    private String name = null;
    private String event = null;
    private String medal = null;
    
    public Athlete( ) {  }
    
    public Athlete( String name, String event, String medal )
    {
        this.name = name;
        this.event = event;
        this.medal = medal;
    }
    
    public void setMedal( String medal ) {  this.medal = medal;  }
    
    public String getMedal( )  {  return medal;  }
    
    public String toString( )
    {
        return name + ":" + event + ":" + medal;
    }
}