using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Evolve : MonoBehaviour {

    public Image current;
    public Sprite jolteon;
    public Sprite flareon;
    public Sprite vaporeon;
    public int electric;
    public int fire;
    public int water;
    public string state;
    
    // Start is called before the first frame update
    void Start() {

        electric = PlayerPrefs.GetInt("Electric");
        fire = PlayerPrefs.GetInt("Fire");
        water = PlayerPrefs.GetInt("Water");
        state = PlayerPrefs.GetString("State");
        if (state == "Electric") {
            current.sprite = jolteon;
        } else if (state == "Fire") {
            current.sprite = flareon;
        } else if (state == "Water") {
            current.sprite = vaporeon;
        }

    } 

    public void Jolteon() {
        
        electric = PlayerPrefs.GetInt("Electric");
        state = PlayerPrefs.GetString("State");
        if (electric > 0 && state == "Eevee") {
            electric -= 1;
            PlayerPrefs.SetInt("Electric", electric);
            current.sprite = jolteon;
            state = "Electric";
            PlayerPrefs.SetString("State", state);
            PlayerPrefs.Save();
        }

    }

    public void Flareon() {
        
        fire = PlayerPrefs.GetInt("Fire");
        state = PlayerPrefs.GetString("State");
        if (fire > 0 && state == "Eevee") {
            fire -= 1;
            PlayerPrefs.SetInt("Fire", fire);
            current.sprite = flareon;
            state = "Fire";
            PlayerPrefs.SetString("State", state);
            PlayerPrefs.Save();
        }

    }

    public void Vaporeon() {
        
        water = PlayerPrefs.GetInt("Water");
        state = PlayerPrefs.GetString("State");
        if (water > 0 && state == "Eevee") {
            water -= 1;
            PlayerPrefs.SetInt("Water", water);
            current.sprite = vaporeon;
            state = "Water";
            PlayerPrefs.SetString("State", state);
            PlayerPrefs.Save();
        }

    }

}