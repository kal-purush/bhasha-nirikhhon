using UnityEngine;
using System.Collections;

public class SpellHintScript : MonoBehaviour {

    public GameObject ArrowsContainer;
    public GameObject[] full_spell;
    public GameObject[] Hint_spells;
    public string key = "";

    string lightningStrike;
    
    bool is_destroyed = true;

    


    // Use this for initialization
    void Start () {

        

        full_spell = new GameObject[5];
        Hint_spells = new GameObject[4];
        
        
        
    }
	
	// Update is called once per frame
	void Update () {
        

        Create_Spell();



        if (Input.GetKeyDown(KeyCode.UpArrow))
        {
            key += "U";
        }
        else if (Input.GetKeyDown(KeyCode.DownArrow))
        {
            key += "D";
        }
        else if (Input.GetKeyDown(KeyCode.LeftArrow))
        {
            key += "L";
        }
        else if (Input.GetKeyDown(KeyCode.RightArrow))
        {
            key += "R";
        }


    }


    void Create_Spell()
    {
        if(key.Length == 6)
            key = "";

        int spellID = gameObject.transform.parent.GetComponent<ComboSystem>().DetectSpellCast(key);
        string spellName = "";

        clear_vector();
        

        for (int i = 0; i < key.Length; i++)
        {
            int keyMatch = 0;
            if (key.Substring(i, 1) == "D")
                keyMatch = 1;
            if (key.Substring(i, 1) == "L")
                keyMatch = 2;
            if (key.Substring(i, 1) == "R")
                keyMatch = 3;
            int myX = -4 + (i*2);
            full_spell[i] = (GameObject)Instantiate(ArrowsContainer.transform.GetChild(0).GetChild(keyMatch).gameObject, new Vector3(transform.parent.transform.position.x + myX, transform.parent.transform.position.y + 5, 0), Quaternion.identity);
        }


        if (spellID != -1 && key.Length > 0)
        {
            spellName = gameObject.transform.parent.GetComponent<ComboSystem>().spellNames[spellID];
            for (int i = key.Length; i < 5; i++)
            {
                int keyMatch = 0;
                if (spellName.Substring(i, 1) == "D")
                    keyMatch = 1;
                if (spellName.Substring(i, 1) == "L")
                    keyMatch = 2;
                if (spellName.Substring(i, 1) == "R")
                    keyMatch = 3;
                int myX = -4 + (i * 2);
                full_spell[i] = (GameObject)Instantiate(ArrowsContainer.transform.GetChild(1).GetChild(keyMatch).gameObject, new Vector3(transform.parent.transform.position.x + myX, transform.parent.transform.position.y + 5, 0), Quaternion.identity);
            }
        }



        for (int u = 0; u < full_spell.Length; u++)
            if (full_spell[u] != null && full_spell[u].transform.parent != this.transform)
            { full_spell[u].transform.parent = this.transform;}

    }


   

   


    void clear_vector()
    {

        for (int j = 0; j < 5; j++)
        {
            Destroy(full_spell[j]);
        }

        is_destroyed = true;
    }
}