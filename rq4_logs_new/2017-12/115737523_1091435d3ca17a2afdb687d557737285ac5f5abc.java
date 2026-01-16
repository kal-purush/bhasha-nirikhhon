import javax.jcr.GuestCredentials;
import javax.jcr.Repository;
import javax.jcr.Session; 
import org.apache.jackrabbit.commons.JcrUtils;

/** 
* First hop example. Logs in to a content repository and prints a 
* status message. 
*/ 
public class FirstHop { 
	

    /** 
    * The main entry point of the example application. 
    * 
    * @param args command line arguments (ignored) 
    * @throws Exception if an error occurs 
    */ 
    public static void main(String[] args) throws Exception { 
        Repository repository = JcrUtils.getRepository();
        Session session = repository.login(new GuestCredentials());
        try { 
            String user = session.getUserID(); 
            String name = repository.getDescriptor(Repository.REP_NAME_DESC); 
            System.out.println( 
            "Logged in as " + user + " to a " + name + " repository."); 
        } finally { 
            session.logout(); 
        } 
    } 
} 