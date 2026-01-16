using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BossCannonBehavior : MonoBehaviour, IOutputModule {

    public Sprite zeroCharge;
    public Sprite oneCharge;
    public Sprite twoCharge;
    public Sprite threeCharge;
    public Sprite fourCharge;

    public GameObject target;
    public GameObject laser;

    public int charges = 0;

    private Sprite[] sprites;

	// Use this for initialization
	void Start () {
        sprites = new Sprite[] { zeroCharge, oneCharge, twoCharge, threeCharge, fourCharge };

        this.GetComponent<SpriteRenderer>().sprite = sprites[charges];
    }

    // Update is called once per frame
    void Update () {

	}

    public void Activate(bool isStart)
    {
        Debug.Log("Triggered");
        if (isStart)
        {
            charges++;
        }
        else
        {
            charges--;
        }
        UpdateSprite();
    }

    public void AttemptFire()
    {
        if(charges == 4)
        {
            laser.GetComponent<LaserBehavior>().Activate(true);
        }
    }

    public void UpdateSprite()
    {
        this.GetComponent<SpriteRenderer>().sprite = sprites[charges];
    }
}