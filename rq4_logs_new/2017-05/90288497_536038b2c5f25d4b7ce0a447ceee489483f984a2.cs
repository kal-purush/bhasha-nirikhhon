using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerHealth : MonoBehaviour {

    public Texture design, sangue;
    public int totalHealth;
    public int currentHealth;

    void Start() {
        currentHealth = totalHealth;
    }
    
    void Damage() {
        currentHealth--;
        if(currentHealth <= 0) {
            // Aqui chama o game over
        }
    }

    void Heal() {
        currentHealth++;
        if(currentHealth > totalHealth) 
            currentHealth = totalHealth;
    }


    public void Die()
    {

    }
    void OnGui () {
        GUI.DrawTexture(new Rect(Screen.width / 40, Screen.height / 40, Screen.width / 5, Screen.height / 8), sangue);
        GUI.DrawTexture(new Rect(Screen.width / 25, Screen.height / 15, Screen.width / 5.5f, Screen.height / 25), design);
    }

}