#nullable enable
using UnityEngine;

namespace Daipan.Enemy.Scripts;

public class EnemySlowDefeatChecker
{
    int SlowDefeatCounter { get; set; }
    readonly int _slowDefeatThreshold = 5; // todo : パラメータで設定できるようにする
    readonly float _slowDefeatCoordinate = 2; // todo 
    

    public void MayIncrementSlowDefeatCounter(Vector3 currentPosition)
    {
        if(_slowDefeatCoordinate < currentPosition.x)
         SlowDefeatCounter++;
    }
    
    /// <summary>
    /// trueの時にslowDefeatCounterをリセットしていることに注意
    /// </summary>
    public bool IsSlowDefeat()
    {
        if(SlowDefeatCounter > _slowDefeatThreshold)
        {
            SlowDefeatCounter = 0;
            return true;
        }

        return false;
    }
}