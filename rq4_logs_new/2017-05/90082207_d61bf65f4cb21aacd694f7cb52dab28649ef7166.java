/**
* Represents an array list collection.
*
* @author						Tom Atwood
* 								EN605.202.81 Data Structures, Spring 2017, Lab #4
* 								May 2, 2017
* @version						1.0.0.0
* @since						1.0.0.0
*/
public class ArrayList<T>
{
    // Instance variables
    private int count = 0;
    @SuppressWarnings("unchecked")
	private T[] items = (T[])new Object[0];

    // Constructors
    /**
     * Constructor with no parameter.
     * @author					Tom Atwood
     * @version					1.0.0.0
     * @since					1.0.0.0
     */
    public ArrayList() {
    }

    // Methods
    /**
     * Converts the list object to an array.
     * @author 					Tom Atwood
     * @version					1.0.0.0
     * @since					1.0.0.0
     * @return					Returns the list items in an object array.
     */
    public T[] toArray() {
        // Returns the underlying array structure.
        return items;
    }

    /**
     * Adds an item to the list.
     * @author 					Tom Atwood
     * @version					1.0.0.0
     * @since					1.0.0.0
     * @param itemToInsert		Object to be added to the list.
     */
    @SuppressWarnings("unchecked")
	public void add(T itemToInsert) {
        // To add an item, create a tempArray one object larger than the underlying array structure (i.e. item)
        T[] tempArray = (T[]) new Object[count + 1];

        // Iterate over underlying array structure and copy to tempArray
        for (int i = 0; i < count; i++) {
            tempArray[i] = items[i];
        }

        // Add item to last slot on tempArray
        tempArray[count] = itemToInsert;

        // Increase count by 1
        count++;

        // Set underlying array structure to equal new expanded array
        items = tempArray;
    }

    /**
     * Updates an item in the list.
     * @author 					Tom Atwood
     * @version					1.0.0.0
     * @since					1.0.0.0
     * @param index				Index value.
     * @param value				Value to update.
     * @throws Exception
     */
    public void update(int index, T value) throws Exception {
        if (index < 0 || index > this.size()) {
            throw new Exception("Requested index " + index + " but size of the list is " + this.size() + ".");
        }
        else {
            items[index] = value;
        }
    }

    /**
     * Prints all items in the list to the console.
     * @author 					Tom Atwood
     * @version					1.0.0.0
     * @since					1.0.0.0
     */
    public void displayAll() {
        // Iterate through all items in underlying array structure and print to the console.
        for (int i = 0; i < count; i++) {
           System.out.println(items[i]);
        }
    }

    /**
     * Removes an object from the list.
     * @author 					Tom Atwood
     * @version					1.0.0.0
     * @since					1.0.0.0
     * @param itemToRemove		Object to be removed from the list.
     */
    @SuppressWarnings("unchecked")
	public void remove(T itemToRemove) {
        // Method variable
        boolean found = false;  // If item found, set to true

        // If item found (via Find method)
        if (!this.find(itemToRemove).equals(null)) {
            // Initialize tempArray one object less than underlying array
            T[] tempArray = (T[]) new Object[count - 1];
            int j = 0; // iterator for tempArray

            // Iterate through underlying array to find item to remove
            for (int i = 0; i < count; i++) {
                // If item is found and has not been found previously
                if ((items[i].equals(itemToRemove)) && (!found)) {
                    // Set found to true and don't copy to tempArray
                    found = true;
                }
                else {
                    //Copy objects from underlying array structure to tempArray
                    tempArray[j] = items[i];
                    // tempArray iterator (i.e. j) will only be different from i if the item has been found
                    j = j + 1;
                }
            }

            // Reduce count by 1
            count = count - 1;

            // Set underlying array structure to be equal to the new reduced tempArray
            items = tempArray;
        }
    }

    /**
     * Remove an item from the list at a specific position.
     * @author 					Tom Atwood
     * @version					1.0.0.0
     * @since					1.0.0.0
     * @param index				Index value.
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
	public void remove(int index) throws Exception {
        if ((index < 0) || (index > this.size())) {
            throw new Exception("Requested index " + index + " but size of the list is " + this.size() + ".");
        }
        else {
            // Initialize tempArray one object less than underlying array
            T[] tempArray = (T[]) new Object[count - 1];

            // Iterate through underlying array to find item to remove
            for (int i = 0; i < count; i++) {
                // If item is found and has not been found previously
                if (i < index) {
                    tempArray[i] = items[i];
                }
                else if (i > index) {
                    tempArray[i - 1] = items[i];
                }
            }

            // Reduce count by 1
            count = count - 1;

            // Set underlying array structure to be equal to the new reduced tempArray
            items = tempArray;
        }
    }

    /**
        * Returns the size of items in the list.
        * @author 					Tom Atwood
        * @version					1.0.0.0
        * @since					1.0.0.0
        * @return					Returns the size of the list.
        */
    public int size() {
        // Return the count of items in the list
        return count;
    }

    public void addRange(ArrayList<T> arrayList) throws Exception {
        for (int i = 0; i < arrayList.size(); i++) {
            this.add(arrayList.get(i));
        }
    }

    /**
        * Finds whether the specified object is within the list.
        * @author 					Tom Atwood
        * @version					1.0.0.0
        * @since					1.0.0.0
        * @param itemToFind			Item sought in the list.
        * @return					If found, returns a copy of the item.  Otherwise, the return value is null.
        */
    public T find(T itemToFind) {
        T returnObject = null;

        // Iterate through underlying array to find the object.
        // If found, return the object.  Else return null.
        for (int i = 0; i < count; i++) {
            if (items[i].equals(itemToFind)) {
                returnObject = items[i];
            }
        }

        return returnObject;
    }

    /**
        * Get the list item at the specific position.
        * @author 					Tom Atwood
        * @version					1.0.0.0
        * @since					1.0.0.0
        * @param index				Item to be sought at specific position in the list.
        * @return					If found, returns a copy of the item.  Otherwise, the return value is null.
        * @throws Exception 
        */
    public T get(int index) throws Exception  {
        T result = null;

        if ((index < 0) || (index > this.size())) {
            throw new Exception("Requested index " + index + " but size of the list is " + this.size() + ".");
        }
        else {
            result = items[index];
        }

        return result;
    }
}