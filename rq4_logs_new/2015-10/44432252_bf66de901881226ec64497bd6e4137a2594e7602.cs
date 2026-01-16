using UnityEngine;
using System.Collections;

public class FireAnimControl : MonoBehaviour {

    void Update()
    {
        GameObject go = GameObject.FindGameObjectWithTag("Player");
        float hp = go.GetComponent<PlayerMovement>().health;
        this.gameObject.GetComponent<SpriteRenderer>().material.color = new Color(this.gameObject.GetComponent<SpriteRenderer>().material.color.r, this.gameObject.GetComponent<SpriteRenderer>().material.color.g,
            this.gameObject.GetComponent<SpriteRenderer>().material.color.b, hp / 400);
    }
}