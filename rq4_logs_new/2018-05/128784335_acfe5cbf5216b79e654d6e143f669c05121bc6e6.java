import java.io.*;
import java.util.*;
/**
 * Characters handles continuing or creating new characters
 * 
 * @author (Cameron Tabion) 
 * @version (4/18/18)
 */
public class Characters
{
    //Variablen gehen hier
    private Scanner user;
    private Scanner file;
    private stats stats;
    private String fileName;
    private boolean invalid;
    private ArrayList<Integer> info;
    private String character;
    private wait w;

    /**
     * Characters Constructor
     */
    public Characters()
    {
        //Initialisierung
        user = new Scanner(System.in);
        stats = new stats();
        fileName = "";
        invalid = true;
        character = "";
        w = new wait();
    }// Ends the Characters Constructor

    /**
     * Method getStats loads stats from file
     */
    public void getStats()
    {
        try
        {
            info = new ArrayList<Integer>();
            file = new Scanner(new File("saves/" + fileName.toLowerCase() + ".txt"));
            // Get Stats
            while (file.hasNextInt())
            {
                info.add(file.nextInt());
            }// Ends the while
        }// Ends the try
        catch (FileNotFoundException e)
        {
            System.out.println("\f" + e.getMessage());
        }// Ends the catch
    }// Ends the getStats Method

    /**
     * Method Characters prompts user to create character and start game
     *
     * @return name - name of the character
     */
    public String Characters() throws InterruptedException
    {
        int classChoice = 0;
        String name = "";

        System.out.print("\fEnter character name: ");
        name = user.nextLine();

        do
        {
            System.out.println("Choose your class");
            System.out.println("1. Berserker\n2. Caster\n3. Assassin\n4. Archer\n5. Knight");
            System.out.print("Choice: ");
            classChoice = user.nextInt();

            // Abhängig von der gewählten Klasse wird eine andere Methode aufgerufen.
            // NOTE THAT I STILL HAVE TO DO ALL CLASS METHODS OTHER THAN KNIGHT CAUSE LAZY
            switch (classChoice)
            {
                case 1:
                character = "berserker";
                invalid = false;
                break;
                case 2:
                character = "caster";
                invalid = false;
                break;
                case 3:
                character = "assassin";
                invalid = false;
                break;
                case 4:
                character = "archer";
                invalid = false;
                break;
                case 5:
                character = "knight";
                invalid = false;
                break;
                default:
                System.out.println("\fInvalid Choice");
                invalid = true;
                break;
            }// Ends the switch
            if (invalid == false) 
            {
                stats.getStats(name, character);
            }// Ends the if
        } while (invalid == true); // Ends the do-while
        return name;
    }// Ends the Characters Class

    /**
     * Method Continue looks for character file and starts game
     *
     * @return fileName - name of the character file
     */
    public String Continue() throws InterruptedException
    {
        System.out.print("\fEnter Character Name: ");
        fileName = user.nextLine();
        String name = fileName.substring(0, 1).toUpperCase() + fileName.substring(1);
        try
        {
            file = new Scanner(new File("saves/" + fileName.toLowerCase() + ".txt"));
            getStats();
            System.out.println("\nFile Found!");
            w.w(500);
            System.out.println("\nName: " + name);
            System.out.println("Level: " + info.get(6));
            System.out.println("Floor Level: " + info.get(5));
            System.out.println("Current Room: " + info.get(10) + " out of " + info.get(9));
            w.w(1500);
        }// Ends the try
        catch (IOException e)
        {
            System.out.println("ERROR: File Not Found");
            w.w(1000);
            System.exit(0);
        }// Ends the catch
        return fileName;
    }// Ends the Continue Method
}//Ende des Programms
