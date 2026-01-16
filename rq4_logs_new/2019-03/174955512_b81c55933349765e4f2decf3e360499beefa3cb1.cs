using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HexPerform
{
    const int WaitFrame = 40;
    HexMaster master;
    int performFrame;

    public HexPerform(HexMaster _master){
        master = _master;
    }

    public void InitWait(){
        performFrame = WaitFrame;
    }

    void EndWait(){
        master.ContactCheck();
    }

    public void Update(){
        if(performFrame <= 0) return;
        performFrame--;
        if(performFrame <= 0){
            EndWait();
        }
    }
}