package grab;

/*
*	Root interface for all types of data systems
*
*	Developed by, Andrew C.
**/

public interface SystemAccessable {
    
    /*
     * Server initialization method
    **/
    boolean init();
    
    /*
     * Direct link to servers core listening method
    **/
    String listenServ();
    
    /*
     * Direct link to send data through server
    **/
    void writeServ(String data); 
    
    /*
     * A safe way to destroy a server object of any type
    **/
    void kill();
        
}