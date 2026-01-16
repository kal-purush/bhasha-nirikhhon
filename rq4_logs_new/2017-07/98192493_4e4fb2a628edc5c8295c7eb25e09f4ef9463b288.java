import java.sql.Connection; // FIXME might not be necessary
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Company extends User
{
    private int companyID;
    private String name;
    private String description;
    private String email;
    private boolean hiring;

    /**
     * @param theirCompanyID: the id of the company that you you want to add as an associate
     * @return: true on success, false otherwise
     */
    public boolean addAssociate( int theirCompanyID )
    {
        // TODO
    }

    /**
     * @param role
     * @param salary
     * @param description
     * @return: true on success, false otherwise
     */
    public boolean postJob( String role, int salary, String description )
    {
        // TODO
    }

    /**
     * @param role
     * @return: true on success, false otherwise
     */
    public boolean deleteJob( String role )
    {
        // TODO
    }

    /**
     * @param city
     * @param state
     * @return: true on success, false otherwise
     */
    public boolean addOffice( String city, String state )
    {
        // TODO
    }

    /**
     * @param city
     * @param state
     * @return: true on success, false otherwise
     */
    public boolean deleteOffice( String city, String state )
    {
        // TODO
    }


    // GETTERS

    public int getCompanyID()
    {
        return this.companyID;
    }

    public String getName()
    {
        return this.name;
    }

    public String getDescription()
    {
        return this.description;
    }

    public String getEmail()
    {
        return this.email;
    }

    public boolean isHiring()
    {
        return this.hiring;
    }

}