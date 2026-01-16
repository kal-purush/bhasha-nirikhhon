using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DamageZone : MonoBehaviour
{
    [SerializeField] private PlayerController playerController;

    #region Unity Method
        private void Awake(){
            GameObject player = GameObject.FindGameObjectWithTag("Player");
            playerController = player.GetComponent<PlayerController>();
        }
    #endregion

    #region Damage

        private void OnTriggerEnter2D(Collider2D collision){
            if (collision.CompareTag("Player"))
            {
                playerController.TakeDamage(400);
                
                Debug.Log("Player Hit!");
            }
        }

    #endregion
}