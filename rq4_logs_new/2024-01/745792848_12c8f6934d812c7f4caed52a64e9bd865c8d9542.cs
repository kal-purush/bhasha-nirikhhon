#nullable enable
using System;
using UnityEngine;

public class InventoryGrid : MonoBehaviour
{
    [Header("Dependencies")]
    
    [SerializeField] private InventorySubsystem? subsystem;
    [SerializeField] private GameObject? slotPrefab;
    
    [Header("State")]
    
    [SerializeField] private Inventory? target;
    [SerializeField] private int slots;

    private void Update()
    {
        if (target != null && slotPrefab != null)
        {
            while (slots < target.GetMaxSlots())
            {
                var slot = Instantiate(slotPrefab, transform).GetComponent<InventorySlot>();
                slot.Initialize(this, target, slots);
                slots++;
            }
        }
    }

    public void Initialize(Inventory inventory)
    {
        target = inventory;
        for (var i = 0; i < transform.childCount; i++) Destroy(transform.GetChild(i));
        slots = 0;
    }

    public bool IsSelected(int index)
    {
        if (subsystem == null) return false;
        return subsystem.GetSelectedInventory() == target && subsystem.GetSelectedIndex() == index;
    }
    
    public void OnIndexClicked(int index)
    {
        if (subsystem != null && target != null)
        {
            subsystem.OnSlotSelected(target, index);
        }
    }
}