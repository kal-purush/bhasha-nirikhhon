/**
 *
 * @author joe
 */
public class Student {
    // random identification number
    private int id;
    
    // first name, last name, academic standing
    private String lname, fname, standing;
    
    // grade point avarage
    private float gpa;
    
    public Student(String fname, String lname, String standing, float gpa, int id) {
        this.fname    = fname;
        this.lname    = lname;
        this.standing = standing;
        this.gpa      = gpa;
        this.id       = id;
    }
    
    // getter methods
    public String getLname()    { return lname; }
    public String getFname()    { return fname; }
    public String getStanding() { return standing; }
    public float getGpa()       { return gpa; }
    public int getId()          { return id; }

    // setter methods
    public void setLname(String lname)       { this.lname = lname; }
    public void setFname(String fname)       { this.fname = fname; }
    public void setStanding(String standing) { this.standing = standing; }
    public void setGpa(float gpa)            { this.gpa = gpa; }
    public void setId(int id)                { this.id = id; }
}