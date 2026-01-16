package org.meltwater.java.dataStructures;

import java.util.NoSuchElementException;
import java.util.TreeSet;

public class Set<Key extends Comparable<Key>> {
	private TreeSet<Key> set;

    /**
     * Initialises an empty set.
     */
    public Set() {
        set = new TreeSet<Key>();
    }

    /**
     * Adds the key to this set (if it is not already present).
     * @param  key the key to add
     * @throws NullPointerException if key is null
     */
    public void add(Key key) {
        if (key == null) throw new NullPointerException("called add() with a null key");
        set.add(key);
    }
    
    /**
     * Returns true if this set contains the given key.
     * @param  key the key
     * @return true if this set contains key and
     *         false otherwise
     * @throws NullPointerException if key is null
     */
    public boolean contains(Key key) {
        if (key == null) throw new NullPointerException("called contains() with a null key");
        return set.contains(key);
    }

    /**
     * Removes the key from this set (if the key is present).
     * @param  key the key
     * @throws NullPointerException if key is null
     */
    public void delete(Key key) {
        if (key == null) throw new NullPointerException("called delete() with a null key");
        set.remove(key);
    }

    /**
     * Returns the number of keys in this set.
     * @return the number of keys in this set
     */
    public int size() {
        return set.size();
    }

    /**
     * Returns true if this set is empty.
     * @return true if this set is empty, and false otherwise
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Returns the largest key in this set.
     * @return the largest key in this set
     * @throws NoSuchElementException if this set is empty
     */
    public Key max() {
        if (isEmpty()) throw new NoSuchElementException("called max() with empty set");
        return set.last();
    }

    /**
     * Returns the smallest key in this set.
     * @return the smallest key in this set
     * @throws NoSuchElementException if this set is empty
     */
    public Key min() {
        if (isEmpty()) throw new NoSuchElementException("called min() with empty set");
        return set.first();
    }


    /**
     * Returns the smallest key in this set greater than or equal to key.
     * @param  key the key
     * @return the smallest key in this set greater than or equal to key
     * @throws NoSuchElementException if there is no such key
     * @throws NullPointerException if key is null
     */
    public Key ceiling(Key key) {
        if (key == null) throw new NullPointerException("called ceiling() with a null key");
        Key k = set.ceiling(key);
        if (k == null) throw new NoSuchElementException("all keys are less than " + key);
        return k;
    }

    /**
     * Returns the largest key in this set less than or equal to key.
     * @param  key the key
     * @return the largest key in this set table less than or equal to key
     * @throws NoSuchElementException if there is no such key
     * @throws NullPointerException if key is null
     */
    public Key floor(Key key) {
        if (key == null) throw new NullPointerException("called floor() with a null key");
        Key k = set.floor(key);
        if (k == null) throw new NoSuchElementException("all keys are greater than " + key);
        return k;
    }

//    /**
//     * Returns the union of this set and that set.
//     * @param  that the other set
//     * @return the union of this set and that set
//     * @throws NullPointerException if that is null
//     */
//    public Set<Key> union(Set<Key> that) {
//        if (that == null) throw new NullPointerException("called union() with a null argument");
//        Set<Key> c = new Set<Key>();
//        for (Key x : this) {
//            c.add(x);
//        }
//        for (Key x : that) {
//            c.add(x);
//        }
//        return c;
//    }

    /**
     * Returns the intersection of this set and that set.
     * @param  that the other set
     * @return the intersection of this set and that set
     * @throws NullPointerException if that is null
     */
    public Set<Key> intersects(Set<Key> that) {
        if (that == null) throw new NullPointerException("called intersects() with a null argument");
        Set<Key> c = new Set<Key>();
        if (this.size() < that.size()) {
            for (Key x : this) {
                if (that.contains(x)) c.add(x);
            }
        }
        else {
            for (Key x : that) {
                if (this.contains(x)) c.add(x);
            }
        }
        return c;
    }
    
    /**
     * This operation is not supported because sets are mutable.
     * @return does not return a value
     * @throws UnsupportedOperationException if called
     */
    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("hashCode() is not supported because sets are mutable");
    }

    /**
     * Unit tests the SET data type.
     */
    public static void main(String[] args) {
        Set<String> set = new Set<String>();

        // insert some keys
        set.add("MEST");
        set.add("MEST");    // overwrite old value
        set.add("2016A");
        set.add("2016B");
        set.add("Generosity");
        set.add("Positivity");
        set.add("Standards");
        
        System.out.println(set.contains("MEST"));
        System.out.println(set.contains("2016B"));
        System.out.println();

        System.out.println(set.ceiling("2016B"));
        System.out.println(set.ceiling("2016A"));
        
        
        System.out.println(set.floor("Generosity"));
        System.out.println(set.floor("Standards"));
        System.out.println();

        for (String string : set) {
        	System.out.println(set);
		}        
    }
}