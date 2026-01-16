using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ObstacleCollision : MonoBehaviour
{
    void OnCollisionEnter(Collision collisionInfo) {
        if(collisionInfo.collider.tag == "Obstacle") {
            Debug.Log("Game Over");
            Application.Quit();
        }
    }

    void OnTriggerEnter(Collider collider) {
        if(collider.gameObject.tag == "Obstacle") {
            Debug.Log("Game Over");
            Application.Quit();
        }
    }
}