using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.Collections.Generic;

public class MatchManager : MonoBehaviour
{
    private int level;
    private int numberOfPlayers;
    private List<int> characters;
    private List<float> charactersLife;
    private List<bool> charactersFrozen;
    private float punctuationTeam1;
    private float punctuationTeam2;
    private float initialTime;
    // Start is called before the first frame update
    void Start()
    {
        level = 1;
        if (GameObject.Find("Player") != null)
        {
            numberOfPlayers++;
        }
        if (GameObject.Find("Player2") != null)
        {
            numberOfPlayers++;
        }
        if (GameObject.Find("Player3") != null)
        {
            numberOfPlayers++;
        }
        if (GameObject.Find("Player4") != null)
        {
            numberOfPlayers++;
        }
        numberOfPlayers = 1;
        charactersLife = new List<float>();
        characters = new List<int>();
        charactersFrozen = new List<bool>();

        charactersLife.Add(100.0f);
        //Uncomment when we have more than one player playing at the same time (Photon multiplayer)
        /*
        charactersLife.Add(100.0f);
        if (this.numberOfPlayers == 4)
        {
            charactersLife.Add(100.0f));
            charactersLife.Add(100.0f);
        }
        */

        charactersFrozen.Add(false);
        //Uncomment when we have more than one player playing at the same time (Photon multiplayer)
        /*
        charactersFrozen.Add(false);
        if (this.numberOfPlayers == 4)
        {
            charactersFrozen.Add(false));
            charactersFrozen.Add(false);
        }
        */

        initialTime = 300.0f;
    }

    // Update is called once per frame
    void Update()
    {

        charactersLife[0] = GameObject.Find("Player").GetComponent<PlayerMovement>().health;
        //Uncomment when we have more than one player playing at the same time (Photon multiplayer)
        /*
        charactersLife[1] = GameObject.Find("Player2").GetComponent<PlayerMovement>().health;
        if (this.numberOfPlayers == 4)
        {
            charactersLife[2] = GameObject.Find("Player3").GetComponent<PlayerMovement>().health;
            charactersLife[3] = GameObject.Find("Player4").GetComponent<PlayerMovement>().health;
        }
        */

        charactersFrozen[0] = GameObject.Find("Player").GetComponent<PlayerMovement>().blocked;
        //Uncomment when we have more than one player playing at the same time (Photon multiplayer)
        /*
        charactersFrozen[1] = GameObject.Find("Player2").GetComponent<PlayerMovement>().blocked;
        if (this.numberOfPlayers == 4)
        {
            charactersFrozen[2] = GameObject.Find("Player3").GetComponent<PlayerMovement>().blocked;
            charactersFrozen[3] = GameObject.Find("Player4").GetComponent<PlayerMovement>().blocked;
        }
        */

        if (numberOfPlayers == 1)
        {
            this.punctuationTeam1 = GameObject.Find("Player").GetComponent<PlayerMovement>().punctuation;
        }
        else if (numberOfPlayers == 2)
        {
            this.punctuationTeam1 = GameObject.Find("Player").GetComponent<PlayerMovement>().punctuation;
            this.punctuationTeam2 = GameObject.Find("Player2").GetComponent<PlayerMovement>().punctuation;
        }
        else if (numberOfPlayers == 4)
        {
            this.punctuationTeam1 = GameObject.Find("Player").GetComponent<PlayerMovement>().punctuation;
            this.punctuationTeam2 = GameObject.Find("Player3").GetComponent<PlayerMovement>().punctuation;
        }
    }

    public int generateOrder(int limit)
    {
        Random.seed = (int)(100 + Time.deltaTime);
        int difficultyLevel = 1;
        int randomLevel;
        for (int i = 0; i < this.level; i++)
        {
            randomLevel = (int)Random.Range(1.0f, limit);
            if (difficultyLevel < randomLevel)
            {
                difficultyLevel = randomLevel;
            }
        }
        return difficultyLevel;
    }

    public void setLevel(int level)
    {
        this.level = level;
    }


    public int getLevel()
    {
        return this.level;

    }


    public void setNumberOfPlayers(int numberOfPlayers)
    {
        this.numberOfPlayers = numberOfPlayers;
    }


    public int getNumberOfPlayers()
    {
        return this.numberOfPlayers;

    }

    public void setCharactersLife(List<float> charactersLife)
    {
        this.charactersLife = charactersLife;
    }


    public List<float> getCharactersLife()
    {
        return this.charactersLife;

    }

    public void setCharacters(List<int> characters)
    {
        this.characters = characters;
    }


    public List<int> getCharacters()
    {
        return this.characters;

    }

    public float getPunctuationTeam1()
    {
        return this.punctuationTeam1;
    }

    public void setPunctuationTeam1(float punctuationTeam1)
    {
        this.punctuationTeam1 = punctuationTeam1;
    }

    public float getPunctuationTeam2()
    {
        return this.punctuationTeam2;
    }

    public void setPunctuationTeam2(float punctuationTeam2)
    {
        this.punctuationTeam2 = punctuationTeam2;
    }

    public float getInitialTime()
    {
        return this.initialTime;
    }

    public void setInitialTime(float initialTime)
    {
        this.initialTime = initialTime;
    }

    public void setCharactersFrozen(List<bool> charactersFrozen)
    {
        this.charactersFrozen = charactersFrozen;
    }


    public List<bool> getCharactersFrozen()
    {
        return this.charactersFrozen;

    }

}