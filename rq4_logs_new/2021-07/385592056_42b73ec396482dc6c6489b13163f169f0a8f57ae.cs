using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyFollowState : IState{
    EnemySM _EnemySM;
    float enemyViewRadius;
    GameObject player;

    public EnemyFollowState(){

    }

    public EnemyFollowState(EnemySM enemySM, float enemyViewRadius, GameObject player){
        this._EnemySM = enemySM;
        this.enemyViewRadius = enemyViewRadius;
        this.player = player;
    }


    public void Enter(){
        
    }

    public void Exit(){
        
    }

    public void FixedTick(){
       
    }

    public void Tick(){
        _EnemySM.transform.position = Vector3.MoveTowards(_EnemySM.transform.position,player.transform.position, 15*Time.deltaTime);
        if(Vector3.Distance(_EnemySM.transform.position,player.transform.position) > enemyViewRadius){
            _EnemySM.ChangeState(_EnemySM.enemyIdleState);
        }
    }
}