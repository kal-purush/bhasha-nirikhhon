import java.util.*;
public class CharNames {
    /**
     * Write a description of class CharNames here.
     *
     * @author (your name)
     * @version (a version number or a date)
     */


        private Map<String, Set> charCatalog;

        /**
         * Constructor tat creates an empty map
         */
        public CharNames()
        {
            charCatalog = new HashMap<>();
        }

        /**
         * method to populate map with data
         */
        public void populateMap()
        {
            Set<String> names = new TreeSet<>();
            names.add("Vayne");
            names.add("Samira");
            names.add("Draven");
            names.add("Catlyn");
            charCatalog.put("Marksman", names);
        }

        /**
         * method to print out the class type and the character
         * in said class
         */
        public void printMap()
        {
            for (Map.Entry<String,Set> entry : charCatalog.entrySet())
            {
                System.out.println("The Characters in the class of" + entry.getKey() + " are: " + entry.getValue());
            }
        }

        /**
         * method to iterate through map and print the key-values
         */
        public void printMapValue(String aClass)
        {
            if(charCatalog.containsKey(aClass)){
                System.out.println(charCatalog.get(aClass));
            } else {
                System.out.println("The character class " + aClass + " does not exist");
            }
        }

        /**
         * method for adding an entry to the map
         * Requires a key(character class) and a set of names(characters
         * that fit the class criteria)
         */
        public void addMapEntry(String aClass, Set charNames)
        {
            if(charCatalog.containsKey(aClass)){
                charCatalog.put(aClass, charNames);
            } else {
                charCatalog.put(aClass, charNames);
            }
        }

        /**
         * deletes a name from a specific character class
         */
        public void deleteFromValueAtKey(String className, String charName)
        {
            if(charCatalog.get(className).contains(charName)){
                charCatalog.get(className).remove(charName);
            }
        }

        public void addToValueAtKey(String className, String charName)
        {
            charCatalog.get(className).add(charName);
        }
    /**
     * Deletes key-pair value of a class by taking the clas type,
     * returning true if it get deleted, flase otherwise.
     */
    public boolean deleteEntry(String theClass) {
        if (charCatalog.containsKey(theClass)) {
            charCatalog.remove(theClass);
            return true;
        } else {
            return false;
        }
    }

    public static void main(String[] args) {
        CharNames aCollection = new CharNames();
        aCollection.populateMap();
        aCollection.printMapValue("Marksman");
    }
}
