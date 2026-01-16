using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Card : MonoBehaviour
{
    public virtual bool Play() { return false; }
    public virtual void AddToObj(GameObject obj){}
}