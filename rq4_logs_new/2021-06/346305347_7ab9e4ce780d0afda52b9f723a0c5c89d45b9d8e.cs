using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ConveyerBelt : Interactable
{
    public Transform[] boxPositions;
    private Chest[] boxes;
    private void Awake()
    {
        boxes = new Chest[boxPositions.Length];
    }
    public override Action[] CalcInteractions()
    {
        if (GameManager.Instance.inventory.IsInInventory(ItemType.Chest) && !(GameManager.Instance.inventory.GetHandItem() as Chest).open)
            return new Action[] { Place };
        return new Action[] { };
    }

    private void Place()
    {
        GetComponent<Animator>().SetTrigger("Start");
        for (int i = 0; i < boxes.Length; i++)
        {
            if (boxes[i] == null)
            {
                boxes[i] = GameManager.Instance.inventory.GetHandItem() as Chest;
                boxes[i].transform.position = boxPositions[i].position;
                boxes[i].gameObject.SetActive(true);
                GameManager.Instance.inventory.DeleteItem(ItemType.Chest);
                return;
            }
        }
        Destroy(boxes[0].gameObject);
        for (int i = 1; i <boxPositions.Length; i++)
        {
            boxes[i - 1] = boxes[i];
            boxes[i] = null;
            boxes[i - 1].transform.position = boxPositions[i - 1].position;
        }
        Place();
    }
}

