using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class PlayerWithEnemy : MonoBehaviour {

    Image HealthBar;
    Image EnergyBar;
    Text bulletText;
    int bulletCount = 0;
    private BoxCollider col;
    float fillHealth = 1.0f;
    float fillEnergy = 1.0f;
    string bulletsString;
    
    // Use this for initialization
    void Start () {
        HealthBar = transform.FindChild("Main Camera").transform.FindChild("Canvas").FindChild("healthBar").GetComponent<Image>();
        EnergyBar = transform.FindChild("Main Camera").transform.FindChild("Canvas").FindChild("energyBar").GetComponent<Image>();
        bulletText = transform.FindChild("Main Camera").transform.FindChild("Canvas").FindChild("bullets").GetComponent<Text>();
        bulletsString = "Bullets :" + bulletCount;
        bulletText.text = bulletsString;
    }
    int i = 0;

    void Awake()
    {

        col = GetComponentInChildren<BoxCollider>();
    }

    // Update is called once per frame
    void Update () {
        i++;
        if (i % 10 == 0)
        {
            fillEnergy -= (float)(Time.deltaTime * 0.2);
            EnergyBar.fillAmount = fillEnergy;
        }
	
	}

    void OnTriggerEnter(Collider other)
    {
        if (other.name == "enemy")
        {
            fillHealth -= 0.1f;
            HealthBar.fillAmount = fillHealth;

        }


        else if (other.name == "bat")
        {
            fillEnergy = 1f;
            EnergyBar.fillAmount = fillEnergy;

        }

        else if (other.name == "bullets")
        {
            bulletCount += 10;
            bulletsString = "Bullets :" + bulletCount;
            bulletText.text = bulletsString;

        }

    }
}