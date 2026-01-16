using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SymbolGet : MonoBehaviour
{
    public void GetSymbol()
    {
        switch (name)
        {
            case "HandScanner 1":
                {
                    GetComponentInParent<CorrectSymbolCheck>().temp = GetComponentInParent<RandomiseSymbols>().symbols[0].GetComponent<SpriteRenderer>().sprite;
                    break;
                }
            case "HandScanner 2":
                {
                    GetComponentInParent<CorrectSymbolCheck>().temp = GetComponentInParent<RandomiseSymbols>().symbols[1].GetComponent<SpriteRenderer>().sprite;
                    break;
                }
            case "HandScanner 3":
                {
                    GetComponentInParent<CorrectSymbolCheck>().temp = GetComponentInParent<RandomiseSymbols>().symbols[2].GetComponent<SpriteRenderer>().sprite;
                    break;
                }
            case "HandScanner 4":
                {
                    GetComponentInParent<CorrectSymbolCheck>().temp = GetComponentInParent<RandomiseSymbols>().symbols[3].GetComponent<SpriteRenderer>().sprite;
                    break;
                }
        }
    }
}