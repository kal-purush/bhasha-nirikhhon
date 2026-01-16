#nullable enable
using System;
using System.Collections.Generic;
using UnityEngine;

[Serializable]
public class Inventory
{
    [SerializeField] private int maxSlots;
    [SerializeField] private List<ItemStack> itemStacks = new();
    
    public Inventory()
    {
    }

    public Inventory(int maxSlots)
    {
        this.maxSlots = maxSlots;
    }

    public ItemStack GetStack(int index) => itemStacks[index];
    
    public int CountItems(ItemType type)
    {
        var ret = 0;
        for (var i = 0; i < GetBound(OperationType.RemoveOrQuery); i++)
        {
            if (itemStacks[i].itemType == type) ret += itemStacks[i].quantity;
        }
        return ret;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="type">Item type to add</param>
    /// <param name="quantity">Amount to add</param>
    /// <returns>Quantity that was not able to be added</returns>
    public int AddItems(ItemType type, int quantity)
    {
        for (var i = 0; i < GetBound(OperationType.Add); i++)
        {
            if (itemStacks[i].itemType == type)
            {
                var added = Mathf.Min(type.stackSize - itemStacks[i].quantity, quantity);
                quantity -= added;
                itemStacks[i] = new ItemStack(type, itemStacks[i].quantity + added);
                if (quantity == 0) return 0;
            }
        }
        
        while(itemStacks.Count < maxSlots) itemStacks.Add(new ItemStack());
        
        for (var i = 0; i < GetBound(OperationType.Add); i++)
        {
            if (itemStacks[i].itemType == null)
            {
                var added = Mathf.Min(type.stackSize, quantity);
                quantity -= added;
                itemStacks[i] = new ItemStack(type, added);
                if (quantity == 0) return 0;
            }
        }
        
        return quantity;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="type">Item type to remove</param>
    /// <param name="quantity">Amount to remove</param>
    /// <returns>Quantity that was successfully removed</returns>
    public int RemoveItems(ItemType type, int quantity)
    {
        var ret = 0;
        for (var i = 0; i < GetBound(OperationType.RemoveOrQuery); i++)
        {
            if (itemStacks[i].itemType == type)
            {
                var removed = Mathf.Min(itemStacks[i].quantity, quantity - ret);
                ret += removed;
                if (removed == itemStacks[i].quantity) itemStacks[i] = new ItemStack();
                else itemStacks[i] = new ItemStack(type, itemStacks[i].quantity - removed);
                if (ret == quantity) return ret;
            }
        }
        return ret;
    }

    private int GetBound(OperationType type)
    {
        return type == OperationType.Add ? maxSlots : itemStacks.Count;
    }

    private enum OperationType
    {
        Add,
        RemoveOrQuery
    }
}