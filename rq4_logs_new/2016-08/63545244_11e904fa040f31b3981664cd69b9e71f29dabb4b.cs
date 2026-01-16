using UnityEngine;
using System.Collections;

public class GlobalContext
{
    private static GlobalContext _instance = null;
    public string TextureName = null;
    public uint RowsCount = 0;
    public uint ColumnsCount = 0;

    public static GlobalContext Instance
    {
        get
        {
            if (_instance == null)
            {
                _instance = new GlobalContext();
            }
            return _instance;
        }
    }

    private GlobalContext()
    {
        
    }

}