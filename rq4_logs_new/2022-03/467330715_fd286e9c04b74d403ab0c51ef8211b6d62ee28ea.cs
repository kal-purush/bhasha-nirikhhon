using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Card : MonoBehaviour
{

    public enum Suit
    {
        Clubs,
        Hearts,
        Diamonds,
        Spades
    }

    public Suit _cardSuit;

    public int _number;

    public Sprite Artwork;
    private SpriteRenderer _spriteRenderer;

    private GameObject _cardSnap;




    public Card(Suit s, int n)
    {
        Debug.Assert(n >= 1 && n <= 13);

        _cardSuit = s;
        _number = n;

    }

    public void SetCard(Suit s, int n)
    {
        _spriteRenderer = GetComponent<SpriteRenderer>();


        _cardSuit = s;
        _number = n;

        string fileName = "";

        switch (_cardSuit)
        {
            case Suit.Clubs:
                fileName += "C";
                break;
            case Suit.Hearts:
                fileName += "H";
                break;
            case Suit.Diamonds:
                fileName += "D";
                break;
            case Suit.Spades:
                fileName += "S";
                break;
            default:
                Debug.LogError("Cad did not contain a valid enum");
                break;

        }

        switch (_number)
        {
            case 11:
                fileName += "J";
                break;
            case 12:
                fileName += "Q";
                break;
            case 13:
                fileName += "K";
                break;
            case 1:
                fileName += "A";
                break;
            default:
                fileName += _number.ToString();
                break;

        }



        fileName = "Textures/Cards/" + fileName;
        Artwork = Resources.Load<Sprite>(fileName);
        _spriteRenderer.sprite = Artwork;
    }

}