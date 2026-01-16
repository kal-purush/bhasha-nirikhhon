using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[System.Serializable]
public class BaseEnemy : MonoBehaviour {
    public enum Enemytype
    {
        small,
        medium,
        large,
        boss
    }
    
    StatsClass stats;
    Enemytype type;
    string enemyName;
    
    public StatsClass Stats
    {
        get
        {
            return stats;
        }

        set
        {
            stats = value;
        }
    }

    public Enemytype Type
    {
        get
        {
            return type;
        }

        set
        {
            type = value;
        }
    }

    public string EnemyName
    {
        get
        {
            return enemyName;
        }

        set
        {
            enemyName = value;
        }
    }

    // Possible Max of 6 actions and move. //Or can be set for each enemy instead of defining them here 
    public virtual void Move() { }
    public virtual void Act1() { }
    public virtual void Act2() { }
    public virtual void Act3() { }
    public virtual void Act4() { }
    public virtual void Act5() { }
}