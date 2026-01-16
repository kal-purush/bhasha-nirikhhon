using UnityEngine;
using System.Collections;
using System.IO;

/// <summary>
/// @author Jake Skov
/// </summary>
public class ScriptImportLevel : MonoBehaviour 
{
    //Reader Variables
    private StreamReader reader;
    private string path;

    //Variables to read in
    private string levelName, authorName;
    private string line;
    private string type;
    private string row, column;
    private string tileNum;

    //Levels
    public static string[] levelNames;

    public void Import()
    {
        ApplyInfo(levelName);
        BuildMap(path);
    }

    void ApplyInfo(string level)
    {
        path = Application.dataPath + "/Maps/Custom/" + level + ".txt";
    }

    //Reads data from the file
    void ReadData()
    {
        reader = new StreamReader(path);

        int lineNum = 0;
        string[] lineData;
        string[] hexCoord;
        using (reader)
        {
            for (int i = 0; i < 500; i++ )
            {
                if (i == 0)
                {
                    levelName = reader.ReadLine();
                } else
                    if(i == 1)
                    {
                        authorName = reader.ReadLine();
                    }
                    else 
                    {
                        //line = reader.ReadLine();

                        //lineData = line.Split("_");
                        //type = lineData[0];

                        //hexCoord = lineData[1].Split(",");
                        //row = hexCoord[0];
                        //column = hexCoord[1];

                        //lineData[2] = tileNum;
                    }
            }
        }
    }

    void BuildMap(string path)
    {
        ReadData();

        //Script Incomplete due to lack of heuristic
    }

    void SwitchStates()
    { 
        
    }
}