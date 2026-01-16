
using UnityEngine;
using System.IO;
using System.Collections.Generic;


public class CSVReader : MonoBehaviour
{
    StreamReader reader = new StreamReader("C:\\Users\\vantan\\Desktop\\作業用ZooGame\\Assets\\hujiwara" + "\\" + "TestSavedata.csv");

    List<List<string>> m_data = new List<List<string>>();

    public const char SplitChar = ',';
    string m_commentString = "//";

    public bool Load(string fileName)
    {
        int counter = 0;
        string line = "";

        while((line = reader.ReadLine()) != null)
        {
            if(line.Contains(m_commentString))
            {
                continue;
            }

            string[] fields = line.Split(SplitChar);
            m_data.Add(new List<string>());

            foreach(var field in fields)
            {
                if(field.Contains(m_commentString) || field == "")
                {
                    continue;
                }
                m_data[counter].Add(field);
            }
            counter++;
        }
        reader.Close();
        return true;
    }

    public void ReaderClose()
    {
        reader.Close();
    }

    public string GetString(int row, int col)
    {
        return m_data[row][col];
    }

    public int GetInt(int row, int col)
    {
        string data = GetString(row, col);
        return int.Parse(data);
    }

    public float GetFloat(int row, int col)
    {
        string data = GetString(row, col);
        return float.Parse(data);
    }

    public bool GetBool(int row, int col)
    {
        string data = GetString(row, col);
        return bool.Parse(data);
    }

    public double GetDouble(int row, int col)
    {
        string data = GetString(row, col);
        return double.Parse(data);
    }
}