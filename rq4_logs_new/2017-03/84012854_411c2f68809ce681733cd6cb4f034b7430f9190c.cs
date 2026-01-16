using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ChargeEnemy : BaseEnemy {
    
    public override IEnumerator Move()
    {
        StartCoroutine(base.Move());    //if player not seen
        yield return null;
    }
    public override void ChooseAttack()
    {
        base.Act4();
        GameStateMachine.enemyCount++;
    }
    public override void InitStats()
    {
        stats = new StatsClass();
        stats.Strength = 5;
    }

    void Start () {
        windup = 1;
        InitStats();
        attackSize = 4; // this is 4 because its the 'T attack'
        attackRange = 4;
        visionRange = 6;
    }
	

	void Update () {
		
	}
}