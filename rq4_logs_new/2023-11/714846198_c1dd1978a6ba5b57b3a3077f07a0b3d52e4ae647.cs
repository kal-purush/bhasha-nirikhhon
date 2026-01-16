using System.Collections;
using System.Collections.Generic;
using TeamB_TD.SaveData;
using UnityEngine;

public class SelectSaveDebug : MonoBehaviour
{
    public void Clear(int num)
    {
        DataManager.Instance.OverWrite(num);
    }

    public void AllClear()
    {
        DataManager.Instance.OverWriteAll();
    }

    public void ClearReset()
    {
        DataManager.Instance.Reset();
    }
}