using System.Collections;
using System.Collections.Generic;
using Unity.VisualScripting.Antlr3.Runtime.Misc;
using UnityEngine;
using UnityEngine.UI;

public class InventoryUI : MonoBehaviour
{
    private int isActive = 0;
    private int selected = -1;
    private GameObject grid;
    private GameObject hand;
    private ThingObjectsList thingsList;
    private ThingIconsList thingsIcons;
    private PlayerInventory playerInventory;
    // Start is called before the first frame update
    void Start()
    {
        grid = GameObject.Find("Grid");
        hand = GameObject.Find("Hand");
        playerInventory = FindObjectOfType<PlayerInventory>();
        thingsList = FindObjectOfType<ThingObjectsList>();
        thingsIcons = FindObjectOfType<ThingIconsList>();
        grid.SetActive(isActive == 1);
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyDown(KeyCode.E))
        {
            isActive ^= 1;
            if (isActive == 1)
            {
                RefreshGridAndHand();
            }
        }
        if (isActive == 1)
        {
            grid.SetActive(isActive == 1);
            int slotId = getSlotId();
            if (slotId == -1)
            {
                return;
            }
            if (slotId != selected)
            {
                if (selected == -1)
                {
                    selected = slotId;
                    return;
                }
                var slot_selected_ = grid.transform.Find("Slot_" + selected).gameObject;
                var slot_selected_bg = slot_selected_.transform.Find("selected").gameObject;
                var new_slot = grid.transform.Find("Slot_" + slotId).gameObject;
                slot_selected_bg.transform.SetParent(new_slot.transform, false);
                selected = slotId;
            } else
            {
                playerInventory.Swap(slotId);
                RefreshGridAndHand();
                var slot_selected_ = grid.transform.Find("Slot_" + selected).gameObject;
                var slot_selected_bg = slot_selected_.transform.Find("selected").gameObject;
                slot_selected_bg.transform.SetParent(slot_selected_.transform, false);
            }
        } else
        {
            RefreshHand();
            grid.SetActive(isActive == 1);
        }
    }

    public void RefreshGridAndHand()
    {
        RefreshGrid();
        RefreshHand();
    }

    public void RefreshGrid()
    {
        for (int i = 0; i < PlayerInventory.InventorySize; i++)
        {
            var slot = grid.transform.Find("Slot_" + i).gameObject;
            var toDestroy = new List<GameObject>();
            foreach (Transform child in slot.transform)
            {
                GameObject c = child.gameObject;
                if (c.name != "selected" && c.name != "background")
                {
                    Debug.Log(c.name);
                    toDestroy.Add(c);
                }
            }
            toDestroy.ForEach(child => Destroy(child));

            if (!playerInventory.IsEmptyOn(i))
            {
                int id = playerInventory.GetOn(i);
                Sprite thingSprite = thingsIcons.thingIcons[id];
          
                var img = new GameObject();
                img.AddComponent<Image>();
                img.GetComponent<Image>().sprite = thingSprite;
                img.GetComponent<Image>().preserveAspect = true;

                img.transform.SetParent(slot.transform, false);
                img.GetComponent<RectTransform>().anchorMin.Set(0, 0);
                img.GetComponent<RectTransform>().anchorMax.Set(1, 1);
            }
        }
    }

    public void RefreshHand()
    {
        var toDestroy = new List<GameObject>();
        foreach (Transform child in hand.transform)
        {
            GameObject c = child.gameObject;
            if (c.name != "background")
            {
                Debug.Log(c.name);
                toDestroy.Add(c);
            }
        }
        toDestroy.ForEach(child => Destroy(child));
        if (!playerInventory.IsEmptyOnHand())
        {
            int id = playerInventory.GetOnHand();
            Sprite thingSprite = thingsIcons.thingIcons[id];
            
            var img = new GameObject();
            img.AddComponent<Image>();
            img.GetComponent<Image>().sprite = thingSprite;
            img.GetComponent<Image>().preserveAspect = true;

            img.transform.SetParent(hand.transform, false);
            img.GetComponent<RectTransform>().anchorMin.Set(0, 0);
            img.GetComponent<RectTransform>().anchorMax.Set(1, 1);
        }
    }

    private int getSlotId()
    {
        if (Input.GetKeyDown(KeyCode.Alpha1)) {
            return 0;
        }
        if (Input.GetKeyDown(KeyCode.Alpha2)) {
            return 1;
        }
        if (Input.GetKeyDown(KeyCode.Alpha3)) {
            return 2;
        }
        if (Input.GetKeyDown(KeyCode.Alpha4)) {
            return 3;
        }
        if (Input.GetKeyDown(KeyCode.Alpha5)) {
            return 4;
        }
        if (Input.GetKeyDown(KeyCode.Alpha6)) {
            return 5;
        }
        if (Input.GetKeyDown(KeyCode.Alpha7)) {
            return 6;
        }
        if (Input.GetKeyDown(KeyCode.Alpha8)) {
            return 7;
        }
        if (Input.GetKeyDown(KeyCode.Alpha9)) {
            return 8;
        }

        return -1;
    }
}