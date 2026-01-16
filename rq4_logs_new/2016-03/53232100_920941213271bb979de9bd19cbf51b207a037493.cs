using UnityEngine;

class PuzzlesManager
{
    public string[,] Table { get; set; }

    private int houseCount = 3;
    private string[] colours = { "White", "Blue", "Red", "Green", "Yellow", "Pink", "Grey", "Black" };
    private string[] pets = { "Cat", "Dog", "Zebra", "Line", "Snake" };
    private string[] drinks = { "Water", "Beer", "Milk", "Juice", "Coffee" };
    private string[] fatherNames = { "Dima", "Slavic", "Max", "Bob", "Leo" };
    private string[] momNames = { "Marina", "Olya", "Natasha", "Sveta", "Marry" };
    
    public PuzzlesManager(int houseCount)
    {
        this.houseCount = houseCount;
        ShuffleArray(colours);
        ShuffleArray(pets);
        ShuffleArray(drinks);
        ShuffleArray(fatherNames);
        ShuffleArray(momNames);
        Table = new string[5, houseCount];
        for (int i = 0; i < houseCount; i++)
        {
            Table[0, i] = colours[i];
            Table[1, i] = pets[i];
            Table[2, i] = drinks[i];
            Table[3, i] = fatherNames[i];
            Table[4, i] = momNames[i];
        }
        LogTable();
    }

    public void LogTable()
    {
        for (int i = 0; i < houseCount; i++)
        {
            Debug.Log(Table[0, i] + " " + Table[1, i] + " " + Table[2, i] + " " + Table[3, i] + " " + Table[4, i]);
        }
    }

    private void ShuffleArray<T>(T[] array)
    {
        for (int i = 0; i < array.Length; i++)
        {
            T temp = array[i];
            int randomIndex = Random.Range(i, array.Length);
            array[i] = array[randomIndex];
            array[randomIndex] = temp;
        }
    }
}