using UnityEngine;
using System.IO;
using System.Collections.Generic;
using System.Text;

public class FoodBuyer : MonoBehaviour
{
    public GameObject foodList;
    FoodList food;

    public GameObject foodIDSetter;
    FoodIDSetter setter;

    int ID;
    
    void Start()
    {
        food = new FoodList();
        food = foodList.GetComponent<FoodList>();

        setter = new FoodIDSetter();
        setter = foodIDSetter.GetComponent<FoodIDSetter>();

        ID = 0;
    }

    public void Sell()
    {
        ID = setter.GetID();

        food.foodList[ID].possessionNumber += 1;

        Saver();

        for(int i = 0; i < 6; ++i)
        {
            Debug.Log(food.foodList[i].possessionNumber);
        }
    }

    void Saver()
    {
        List<List<int>> data = new List<List<int>>();
        for(int i = 0; i < 6; ++i)
        {
            List<int> line = new List<int>();
            line.Add(food.foodList[i].ID);
            line.Add(food.foodList[i].possessionNumber);
            data.Add(line);
        }
        MapCSVSaver(data, Application.persistentDataPath + "/", "sample.csv");
    }

    void MapCSVSaver(List<List<int>> data, string directory, string path)
    {
        string save = string.Empty;
        for(int i = 0; i < data.Count-1; ++i)
        {
            save += LineCSVMaker(data[i]);
            save += "\n";
        }
        save += LineCSVMaker(data[data.Count - 1]);

        Writer(save, directory, path);
    }

    string LineCSVMaker(List<int> data)
    {
        string csv = string.Empty;

        for(int i = 0; i < data.Count-1; ++i)
        {
            csv += data[i].ToString();
            csv += ",";
        }
        csv += data[data.Count - 1].ToString();

        return csv;
    }

    void Writer(string data, string directory, string path)
    {
        using (var stream = new FileStream(directory + path, FileMode.OpenOrCreate))
        {
            using (var writer = new StreamWriter(stream, Encoding.UTF8))
            {
                writer.WriteLine(data);
            }
        }
    }
}