package kings.cs.game;

import java.util.HashMap;
import java.util.Iterator;
/**
 * Represents a book. 
 * 
 * @author Courtney Rikoskie
 * @version Spring 2017
 */
public class Book extends Item implements Readable {
    /** The pages and the information contained of each page. */
    private HashMap<String, String> pages;
    /** The intro text of the book. */
    private String introText;
    
    /**
     * Constructs a new Book.
     * 
     * @param theName The name of the book.
     * @param theDescription The description of the book.
     * @param thePointValue The point value of the book.
     * @param theWeight The weight (in ounces) of the book.
     * @param theIntro The intro text of the book.
     */
    public Book(String theName, String theDescription, int thePointValue, double theWeight, String theIntro) {
        super(theName, theDescription, thePointValue, theWeight);
        pages = new HashMap<String, String>();
        introText = theIntro;
    }
    
    /**
     * Adds the specified page to the book.
     * 
     * @param page The specified page.
     * @param information The information of the specified page.
     */
    public void addPage(String page, String information) {
        pages.put(page, information);
    }
    
    /**
     * Gets the first page of the book and the content within it.
     * 
     * @return The first page of the book and the content within it.
     */
    public String read() {
        String result = "You look at the first page.\n\n";
        result += introText;
           
        Iterator<String> iter = pages.keySet().iterator();
            
        while (iter.hasNext()) {
            String current = iter.next();
                
            result += "     " + current + "\n";
        }
        
        return result;
    }
    
    /**
     * Gets the content of the specified page.
     * 
     * @param page The specified page.
     * @return The content of the specified page
     */
    public String goTo(String page) {
        Iterator<String> iter = pages.keySet().iterator();
        String result = "That page doesn't exist.";
        boolean found = false;
        
        while (iter.hasNext() && !found) {
            String current = iter.next();
            
            if (current.equals(page)) {
                result = "You read the page.\n\n" + pages.get(page);
                found = true;
            }
        }
        
        return result;
    }
}