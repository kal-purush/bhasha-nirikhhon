using System;
using UnityEngine;

public class Helmet : Interactable, IHideable, IHideable.IWearable
{
    public HelmetPlace helmetPlace;
    public SpeechTextSO cantWearText;
    public override Action[] CalcInteractions()
    {
        return new Action[] { Wear };
    }

    /**
     * Wear the helmet
     * TODO need to add a visual (maybe not add to inventory? or create some kind of visual inventory)
     */
    public void Wear()
    {
        if (!GameManager.Instance.inventory.IsInInventory(ItemType.Helmet) && GameManager.Instance.inventory.CanAdd())
        {
            PickUp();
            return;
        }
        GameManager.Instance.SpeechManager.StartSpeech(transform.position, cantWearText);
    }

    public void PickUp()
    {
        GameManager.Instance.inventory.AddItem(this);
    }
}