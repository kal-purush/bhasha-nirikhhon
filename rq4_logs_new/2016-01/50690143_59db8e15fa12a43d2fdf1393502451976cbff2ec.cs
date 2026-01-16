using UnityEngine;
using System.Collections;

public class ComboSystem : MonoBehaviour {

    // Variables
    public string key = "";
    public System.Collections.Generic.Dictionary<string, System.Action> spells;
    private float timeUser = 0;
    public float comboLimit;
    

    // Use this for initialization
    void Start () {
      spells = new System.Collections.Generic.Dictionary<string, System.Action>()
      {
          {"UDLLR", () => fireCombo() },
          {"RRLUD", () => waterCombo() }
      };


        randomizeSpells();
    }

    // Update is called once per frame
    void Update()
    {

        // We check if the time between keys is greater than specific amount
        timeUser += Time.deltaTime;

        // Check if the user has stop input key during a specified time
        if (timeUser > comboLimit)
        {
            key = "";
            timeUser = 0;
        }

        // Check for the Key Input pressed, we check the four arrow keys
        if (Input.GetKeyDown(KeyCode.UpArrow))
        {
            key += "U";
            timeUser = 0;
        }
        else if (Input.GetKeyDown(KeyCode.DownArrow))
        {
            key += "D";
            timeUser = 0;
        }
        else if (Input.GetKeyDown(KeyCode.LeftArrow))
        {
            key += "L";
            timeUser = 0;
        }
        else if (Input.GetKeyDown(KeyCode.RightArrow))
        {
            key += "R";
            timeUser = 0;
        }

        // We check if the accumulated string is the same as element combo
        if (spells.ContainsKey(key))
        {
            spells[key].Invoke();
            key = "";
        }
    }

    void randomizeSpells()
    {
        Random.seed = (int)System.DateTime.Now.Ticks;
        var newSpells = new System.Collections.Generic.Dictionary<string, System.Action>();
        string[] charOptions = { "U", "D", "L", "R" };
        foreach (var kvp in spells)
        {
            string spellName = "";
            do
            {
                while (spellName.Length < 5)
                {
                    spellName += charOptions[(int)(Random.value * 4)];
                }

            } while (newSpells.ContainsKey(spellName));
            newSpells.Add(spellName, kvp.Value);

            Debug.Log("New Spell Added : " + spellName + " -> ");
            kvp.Value.Invoke();
        }

        spells = newSpells;
    }

    void waterCombo()
    {
        Debug.Log("Water Spell !");
        //Todo Water spell effects
    }

    void fireCombo()
    {
        Debug.Log("Fire Spell !");
        //Todo fire spell effects
    }

}