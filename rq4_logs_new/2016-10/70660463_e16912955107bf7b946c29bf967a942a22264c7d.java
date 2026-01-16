
/**
 * 
 * Emily Zhuang
 * APCS:Period 4
 * Mr. Lantsberger
 * 11 October 2016
 */

public class Palindrome
{
    String lowerPal, output;   
    int length, character, back, punct;
        
    
    public Palindrome(String pal)
    {
        lowerPal = pal.toLowerCase();
        length = lowerPal.length(); 
        character = 0;
        back = 1;
    }
    
    public String testPal()
    {
        if (length == 1) 
            output = "No, the string you entered is NOT a palindrome. ";
            
        else
        {
            // remove spaces and punctuation
            punct = lowerPal.indexOf(" ");
            while (punct != -1)
            {
                length = lowerPal.length(); 
                lowerPal = lowerPal.substring (0, punct) + lowerPal.substring ((punct + 1), (length));
                punct = lowerPal.indexOf(" ");
            }
            punct = lowerPal.indexOf(",");
            while (punct != -1)
            {
                length = lowerPal.length(); 
                lowerPal = lowerPal.substring (0, punct) + lowerPal.substring ((punct + 1), (length));
                punct = lowerPal.indexOf(",");
            }
            punct = lowerPal.indexOf(".");
            while (punct != -1)
            {
                length = lowerPal.length(); 
                lowerPal = lowerPal.substring (0, punct) + lowerPal.substring ((punct + 1), (length));
                punct = lowerPal.indexOf(".");
            }
            punct = lowerPal.indexOf("!");
            while (punct != -1)
            {
                length = lowerPal.length(); 
                lowerPal = lowerPal.substring (0, punct) + lowerPal.substring ((punct + 1), (length));
                punct = lowerPal.indexOf("!");
            }
            punct = lowerPal.indexOf("?");
            while (punct != -1)
            {
                length = lowerPal.length(); 
                lowerPal = lowerPal.substring (0, punct) + lowerPal.substring ((punct + 1), (length));
                punct = lowerPal.indexOf("?");
            }
            punct = lowerPal.indexOf(";");
            while (punct != -1)
            {
                length = lowerPal.length(); 
                lowerPal = lowerPal.substring (0, punct) + lowerPal.substring ((punct + 1), (length));
                punct = lowerPal.indexOf(";");
            }
            punct = lowerPal.indexOf(":");
            while (punct != -1)
            {
                length = lowerPal.length(); 
                lowerPal = lowerPal.substring (0, punct) + lowerPal.substring ((punct + 1), (length));
                punct = lowerPal.indexOf(":");
            }
            punct = lowerPal.indexOf("-");
            while (punct != -1)
            {
                length = lowerPal.length(); 
                lowerPal = lowerPal.substring (0, punct) + lowerPal.substring ((punct + 1), (length));
                punct = lowerPal.indexOf("-");
            }
                
            length = lowerPal.length(); 
                
            //Compare the first char to the last char and so on.
            while ((character < ((double) length / 2)) && (lowerPal.charAt(character) == lowerPal.charAt(length - back)))
            {
                character++;
                back++; 
            }
                
            if ((character - 1) == (length / 2))
                output = "Yes, the string you entered is a palindrome. ";
            else
                output = "No, the string you entered is NOT a palindrome. ";
            }

            
        return output;

        }


    }
