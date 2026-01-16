using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
public class CardManager : MonoBehaviour {
    GameObject cardSelected;
    public List<CardInfo> playerDeck = new List<CardInfo>();
    public List<CardInfo> lootDeck = new List<CardInfo>();
    public List<CardInfo> discardPile = new List<CardInfo>();
    public List<GameObject> pHand = new List<GameObject>();
    public List<Recipe> recipeList = new List<Recipe>();
    public int randomNum;
    bool forgeable = false;
    public GameObject forge1;
    public GameObject forge2;
    GameManager gm;
    void Start()
    {
        gm = GameManager.gm;
        if (cardSelected == null) { }
        for (int i = 0; i < 4; i++)
        {
            pHand[i].SetActive(false);
        }
        
    }
    void Update()
    {
        Controls();
        //DrawDeck();
    }
    public void Controls()
    {
        if (Input.GetKeyDown(KeyCode.LeftShift))
        {
            Forging();
        }
        if (Input.GetKeyDown(KeyCode.A))
        {
            UseCard(0);
        }

        if (Input.GetKeyDown(KeyCode.S))
        {
            UseCard(1);
        }

        if (Input.GetKeyDown(KeyCode.D))
        {
            UseCard(2);
        }

        if (Input.GetKeyDown(KeyCode.F))
        {
            UseCard(3);
        }
        if (Input.GetKeyDown(KeyCode.X))
        {
            DrawCard(lootDeck);
            //gm.token++;
        }
        if (Input.GetKeyDown(KeyCode.Q))
        {
            DrawCard(discardPile);
        }
        if (Input.GetKeyDown(KeyCode.Space))
        {
            if (playerDeck.Count != 0)
                DrawCard(playerDeck);
            else if(playerDeck.Count == 0 && discardPile.Count > 0)
            {
                for (int i = 0; i < discardPile.Count; i++)
                {
                    playerDeck.Add(discardPile[i]);

                }

                discardPile.Clear();
                DrawCard(playerDeck);
            }
        }
    }

    public void UseCard(int i)
    {
        GameObject o = pHand[i];

        if (o.activeInHierarchy == true && forgeable == false)
        {
            CardInfo c = o.GetComponent<CardTemplate>().card;

            pHand[i].SetActive(false);
            discardPile.Add(o.GetComponent<CardTemplate>().card);
            switch (c.title)
            {
                case "Slash":
                    gm.actionText.text = c.title;
                    //enter dmg calculator
                    break;
                case "Dash":
                    gm.actionText.text = c.title;
                    break;
                case "Beam":
                    gm.actionText.text = c.title;
                    break;
                case "DashBeam":
                    gm.actionText.text = c.title;
                    break;
                default:
                    break;
            }
        }
        if (o.activeInHierarchy == true && forgeable == true)
        {
            if (forge1 == null)
            {
                forge1 = o;
                return;
            }
            else if (forge1 != null)
            {
                forge2 = o;
                ForgeCard(forge1, forge2);
            }
        }
    }

    public void DrawCard(List<CardInfo> list)
    {
        Debug.Log(list.Count);  
        if (list.Count > 0)
        {
            for (int i = 0; i < 4; i++)
            {
                if (pHand[i].activeInHierarchy == false)
                {

                    int x = Random.Range(0, list.Count);
                    CardInfo c = list[x];
                    if (list != lootDeck)
                    {
                        int d = list.Count;
                        Debug.Log("List Count: " + d);
                        list.RemoveAt(x);
                    }
                    pHand[i].SetActive(true);
                    cardSelected = pHand[i];
                    cardSelected.GetComponent<CardTemplate>().CardUpdate(c);
                    if (list == lootDeck)
                    {
                        gm.TokenUpdate(1);
                        gm.actionText.text = "Draw from Loot Deck";
                    }
                    if (list == playerDeck)
                    {
                        
                        gm.actionText.text = "Draw from Player Deck";
                    }
                    if (list == discardPile)
                    {
                        
                        gm.actionText.text = "Draw from Discard Deck";
                    }
                    return;
                }
            }
        }
        else { return; }
    }
    public void ForgeCard(GameObject a, GameObject b)
    {
        CardInfo c = a.GetComponent<CardTemplate>().card;
        CardInfo d = b.GetComponent<CardTemplate>().card;
        
        for (int i = 0; i < recipeList.Count; i++)
        {
            Recipe r = recipeList[i];
            if (c == r.card1 && d == r.card2 && gm.token >= r.reqToken
                || c == r.card2 && d == r.card1 && gm.token >= r.reqToken )
            {
                gm.actionText.text = recipeList[i].name + " Forging Complete";
                gm.TokenUpdate(-r.reqToken);
                a.GetComponent<CardTemplate>().CardUpdate(recipeList[i].fusedCard);
                b.SetActive(false);
                Forging();
                return;
            }
        }         
        Forging();
        return;
    }
    public void Forging()
    {
        forgeable = !forgeable;
        if (forgeable == true)
            gm.forgeable.text = "Forgeable : " + forgeable;
        if (forgeable == false)
        {
            forge1 = null;
            forge2 = null;
            gm.forgeable.text = "Forgeable : " + forgeable;
        }
    }
    //public void DrawDeck()
    //{
    //    if (Input.GetKeyDown(KeyCode.Space) && playerDeck.Count != 0)
    //    {
    //        DrawCard(playerDeck);
    //    }
    //    else if (Input.GetKeyDown(KeyCode.Space) && playerDeck.Count == 0 && discardPile.Count > 1)
    //    {
    //        for (int i = 0; i < discardPile.Count; i++)
    //        {
    //            playerDeck.Add(discardPile[i]);

    //        }

    //        discardPile.Clear();
    //        DrawCard(playerDeck);
    //    }
    //}
}