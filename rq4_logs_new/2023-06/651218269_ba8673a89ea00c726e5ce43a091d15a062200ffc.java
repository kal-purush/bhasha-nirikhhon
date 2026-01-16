package datastructures.maps;

import java.util.ArrayList;

/**
 * Class which represents the part of the input - date.
 * Form: YYYY-MM-DD
 */
class Date {
    String date;

    Date(String dateRec) {
        this.date = dateRec;
    }

    @Override
    public String toString() {
        return date;
    }
}

/**
 * Class which implements Entry<Ids, Cost>.
 * Ids - the array of customers' ids.
 * Cost - integer value of every purchase.
 */
class IdAndCost {
    float cost;
    ArrayList<String> ids = new ArrayList<>();

    IdAndCost(String idRec, String costRec) {
        this.ids.add(idRec);
        this.cost = Float.parseFloat(costRec.substring(1));
    }
}

/**
 * The Entry<K, V> for further HashMap implementation.
 *
 * @param <K> - object which will be treated as key in HashMap.
 * @param <V> object which will be treated as value in HashMap.
 */
class KeyValuePair<K, V> {
    private final K key;
    private final V value;

    KeyValuePair(K keyReceived, V valueReceived) {
        this.key = keyReceived;
        this.value = valueReceived;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }
}

/**
 * Interface with all canonical functions for MapADT.
 *
 * @param <K> - object which will be treated as key in HashMap.
 * @param <V> - object which will be treated as value in HashMap.
 */
interface MapADT<K, V> {
    int size();

    boolean isEmpty();

    V get(K key);

    void put(K key, V value);

    void remove(K key);

    Iterable<K> keySet();

    Iterable<V> values();

    Iterable<KeyValuePair<K, V>> entrySet();
}

/**
 * The main class, which implements the functionality of HashMap through the Open Addressing with Double Hashing.
 *
 * @param <K> - object which will be treated as key in HashMap.
 * @param <V> - object which will be treated as value in HashMap.
 */
class HashMap<K, V> implements MapADT<K, V> {
    /**
     * Minimal prime number for any HashMap.
     * <p>
     * Will be needed as a HashStep.
     */
    private final int minPrime = 3;

    /**
     * Factor of rehashing. That means that HashTable size multiply by 4 everytime when HashTable is full.
     */
    private final int rehashingSize = 4;

    /**
     * Initial size for HashTable.
     */
    private final int maxHashtableSize = 100;
    int size;
    KeyValuePair<K, V>[] hashMap;
    private int closestLowerPrime;

    HashMap() {
        size = maxHashtableSize;
        closestLowerPrime = getClosestLowerPrime(size);
        hashMap = new KeyValuePair[size];
    }

    /**
     * Get the current size of the HashTable.
     *
     * @return int - size of HashTable.
     */
    @Override
    public int size() {
        return size;
    }

    /**
     * Check if the HashTable is empty.
     *
     * @return boolean - true if Hashtable is empty; Otherwise, false.
     */
    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Get the value from the HashMap according to received key.
     * Complexity: average case - O(1 / (1 - loadFactor)).
     *
     * @param key - object which will be treated as key in HashMap.
     * @return V obj - object which is value for key in current Hashmap;
     * <p>
     * Otherwise, if no such key exists in HashMap - null.
     */
    @Override
    public V get(K key) {
        int startingIndex = getStartIndexWithHashing(key);
        int hashStep = getSpecifiedHashStep(key);

        for (int i = 0; i < size(); i++) {
            int currentIndex = (startingIndex + i * hashStep) % size();

            if (hashMap[currentIndex] == null) {
                return null;
            }

            if (hashMap[currentIndex].getKey().equals(key)) {
                return hashMap[i].getValue();
            }
        }

        return null;
    }

    /**
     * Modified version of insert function for HashMap.
     * In this program, we treat date of the purchase as a key.
     * Hence, if no such date already inserted in HashMap - insert it as first.
     * Otherwise, if such date exists in HashMap - resubmit the V value for current key.
     * (Sum of costs and adding ID in IDs array).
     * Last option, if HashTable is full --> rehash it.
     * Complexity: average case O(1 / (1 - loadFactor))
     *
     * @param key   - object which will be treated as key in HashMap.
     * @param value - object which will be treated as value in HashMap.
     */
    @Override
    public void put(K key, V value) {
        int startingIndex = getStartIndexWithHashing(key);
        int hashStep = getSpecifiedHashStep(key);
        boolean noEmptySlots = true;

        for (int i = 0; i < size(); i++) {
            int currentIndex = (startingIndex + i * hashStep) % size();

            if (hashMap[currentIndex] == null) {
                noEmptySlots = false;
                hashMap[currentIndex] = new KeyValuePair<>(key, value);
                break;
            }

            if (hashMap[currentIndex].getKey().equals(key)) {
                noEmptySlots = false;
                /*
                Reassign the value for this date (key) if we received from the input the same date.
                 */
                IdAndCost idAndCost = (IdAndCost) hashMap[currentIndex].getValue();
                idAndCost.cost += ((IdAndCost) value).cost;
                idAndCost.ids.add(((IdAndCost) value).ids.get(0));
                hashMap[currentIndex] = new KeyValuePair<>(key, (V) idAndCost);
                break;
            }
        }

        if (noEmptySlots) {
            rehash();
            put(key, value);
        }
    }

    @Override
    public void remove(K key) {
    }

    @Override
    public Iterable<K> keySet() {
        return null;
    }

    @Override
    public Iterable<V> values() {
        return null;
    }

    /**
     * Get the set of KeyValuePair<K, V>.
     *
     * @return List() - the set of KeyValuePair<K, V>
     */
    @Override
    public Iterable<KeyValuePair<K, V>> entrySet() {
        ArrayList<KeyValuePair<K, V>> arr = new ArrayList<>();

        for (int i = 0; i < size(); i++) {
            if (hashMap[i] != null) {
                arr.add(hashMap[i]);
            }
        }

        return arr;
    }

    /**
     * Rehash the table with factor == 4. (Increase the size 4 times).
     */
    private void rehash() {
        size *= rehashingSize;
        closestLowerPrime = getClosestLowerPrime(size);

        KeyValuePair<K, V>[] oldHashMap = hashMap.clone();
        this.hashMap = new KeyValuePair[size];

        for (int i = 0; i < size / rehashingSize; i++) {
            if (oldHashMap[i] != null) {
                put(oldHashMap[i].getKey(), oldHashMap[i].getValue());
            }
        }
    }

    /**
     * Get the closest prime for less than num.
     *
     * @param num - number
     * @return int - closest prime les than num.
     */
    private int getClosestLowerPrime(int num) {
        num--;
        while (!isPrime(num)) {
            num--;
        }

        return Math.max(num, minPrime);
    }

    /**
     * Check if the number is prime.
     *
     * @param num - int number.
     * @return boolean - true if number is prime; Otherwise, false.
     */
    private boolean isPrime(int num) {
        for (int i = 2; i < (int) Math.sqrt(num) + 1; i++) {
            if (num % i == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Function which implements the function for finding the starting index.
     * Double Hashing: initial index (starting).
     *
     * @param key - object which will be treated as key in HashMap.
     * @return int initial index.
     */
    private int getStartIndexWithHashing(K key) {
        return (key.hashCode() & 0xfffffff) % size;
    }

    /**
     * Function which implements the function for finding the hashing step.
     * Double Hashing: hash step.
     *
     * @param key - object which will be treated as key in HashMap.
     * @return int hashStep.
     */
    private int getSpecifiedHashStep(K key) {
        return closestLowerPrime - ((key.hashCode() & 0xfffffff) % closestLowerPrime);
    }
}