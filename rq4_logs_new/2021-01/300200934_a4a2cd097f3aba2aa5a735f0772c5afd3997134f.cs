using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEditor;
using System;

public class ExportData : MonoBehaviour
{
    public static void exportData(List<string> data, string filepath)
    {
        try
        {
            System.IO.StreamWriter file = new System.IO.StreamWriter(@filepath,true);
            foreach (String line in data)
                file.WriteLine(line);

            file.Close();

        }
        catch (Exception e)
        {
            throw(e);
        }
    }
}