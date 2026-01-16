using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WeaponPickupDecider : MonoBehaviour {

    SpriteRenderer spriteRenderer;
    List<IHoldable> weaponOptions;
    public string Choice { get; set; }

	// Use this for initialization
	void Start () {
        int weaponIndex = Random.Range(0, 3);
        spriteRenderer = this.GetComponent<SpriteRenderer>();
        Debug.Log("Weapon Pickup Choice was: " + weaponIndex);
        switch (weaponIndex)
        {
            case 0:
                // Pencil
                spriteRenderer.color = Color.black;
                this.Choice = "Pencil";
                break;
            case 1:
                // Paper
                spriteRenderer.color = Color.white;
                this.Choice = "Paper";
                break;
            case 2:
                // Textbook
                spriteRenderer.color = Color.blue;
                this.Choice = "Textbook";
                break;
            case 3:
                // Ruler
                spriteRenderer.color = Color.red;
                this.Choice = "Ruler";
                break;
        }

	}
	
	// Update is called once per frame
	void Update () {
        this.GetComponent<Rigidbody2D>().MoveRotation(this.GetComponent<Rigidbody2D>().rotation + 1);
	}
}