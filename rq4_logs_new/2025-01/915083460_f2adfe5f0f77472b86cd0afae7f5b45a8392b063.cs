using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FlyBallController : MonoBehaviour{
    [SerializeField, ReadOnly] private FlyBallMovement flyBallMovement;
    [SerializeField] private FlyBallStat flyBallStat;

    [SerializeField, ReadOnly] private PlayerController playerController;
    [SerializeField, ReadOnly] private GameObject player;
    [SerializeField, ReadOnly] private TrailRenderer trailRenderer;
    #region Unity Method
        private void Awake(){
            player = GameObject.FindGameObjectWithTag("Player");
            playerController = player.GetComponent<PlayerController>();
            trailRenderer = GetComponent<TrailRenderer>();

            flyBallMovement = GetComponent<FlyBallMovement>();
        }
        
        private void Update(){
            if (flyBallMovement.time > 1f){
                flyBallMovement.MoveToPlayer(player);
            } else {
                flyBallMovement.MoveRandom();
            }
        }

        public void Initialize(){
            flyBallMovement.Initialize(trailRenderer);
        }
    #endregion

    #region Check Distance
        public void CheckDistancePlayer(Vector3 targetPosition){
            if  (Vector3.Distance(transform.position, targetPosition) < 0.1f){
                gameObject.SetActive(false);

                playerController.Heal(25);
            }
        }
    #endregion
}