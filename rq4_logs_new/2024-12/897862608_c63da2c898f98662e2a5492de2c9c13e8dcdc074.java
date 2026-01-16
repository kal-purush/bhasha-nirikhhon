import java.util.Random;

public class SimpleUniformHashing {
    private int[] hashTable; // Die Tabelle, in der die Werte gespeichert werden
    private int size; // Anzahl der Plätze (Buckets) in der Tabelle

    public SimpleUniformHashing(int size) {
        this.size = size;
        this.hashTable = new int[size];
    }

    // Hash-Funktion: Wandelt einen Schlüssel in einen Index um
    public int hash(int key) {
        // Verwende den Schlüssel, um eine zufällige Zahl zu erzeugen
        Random random = new Random(key);
        int randomValue = random.nextInt(); // Erzeuge eine Zufallszahl
        return Math.abs(randomValue) % size; // Begrenze sie auf die Größe der Tabelle
    }

    // Füge einen Schlüssel in die Hash-Tabelle ein
    public void insert(int key) {
        int index = hash(key); // Berechne den Index mit der Hash-Funktion
        hashTable[index] = key; // Speichere den Schlüssel im berechneten Index
        System.out.println("Key " + key + " hashed to index " + index);
    }

    // Zeige den Inhalt der Hash-Tabelle
    public void display() {
        for (int i = 0; i < size; i++) {
            System.out.println("Bucket " + i + ": " + (hashTable[i] == 0 ? "Empty" : hashTable[i]));
        }
    }

    public static void main(String[] args) {
        SimpleUniformHashing hashing = new SimpleUniformHashing(10); // Erstelle eine Hash-Tabelle mit 10 Buckets

        // Füge Schlüssel hinzu
        hashing.insert(15);
        hashing.insert(23);
        hashing.insert(42);
        hashing.insert(56);

        // Zeige die Hash-Tabelle
        hashing.display();
    }
}