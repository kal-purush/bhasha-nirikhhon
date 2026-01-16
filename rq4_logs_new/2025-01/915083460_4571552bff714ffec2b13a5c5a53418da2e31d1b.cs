using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class New_PlayerController : MonoBehaviour{
    [SerializeField, ReadOnly] public float horizontalInput;

    [SerializeField, ReadOnly] private New_PlayerAnimation playerAnimation;
    [SerializeField, ReadOnly] private New_PlayerMovement playerMovement;
    [SerializeField, ReadOnly] private New_PlayerStat playerStat;


    #region Unity Method
        private void Awake(){
            playerAnimation = GetComponent<New_PlayerAnimation>();
            playerMovement = GetComponent<New_PlayerMovement>();
            playerStat = GetComponent<New_PlayerStat>(); 
        }

        private void Update(){
            horizontalInput = Input.GetAxis("Horizontal");
            playerMovement.FlipPlayer(horizontalInput);
            playerMovement.MovePlayer(horizontalInput);
        }
    #endregion
}