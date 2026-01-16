using UnityEngine;
using System.Collections;
using System;

public class Collectable : IItems, IComparable
{
    public int id { get; set; }
    public int groupId { get; set; }
    public string itemName { get; set; }
    public string description { get; set; }
    public string type { get; set; }
    public string slug { get; set; }
    public Sprite sprite { get; set; }

    public Collectable(int groupId, int id, string itemName, string description, string slug)
    {
        this.groupId = groupId;
        this.id = id;
        this.itemName = itemName;
        this.description = description;
        this.slug = slug;
        this.sprite = Resources.Load<Sprite>("Sprites/Items/" + slug);
    }

    public Collectable()
    {
        id = -1;
    }

    public int CompareTo(object obj)
    {
        if (obj is Collectable)
        {
            Collectable compareObj = (Collectable)obj;
            int returnValue = groupId.CompareTo(compareObj.groupId);
            if (returnValue == 0)
            {
                return this.id.CompareTo(compareObj.id);
            }
            return returnValue;
        }
        else
        {
            throw new ArgumentException("Object not a Item");
        }
    }
}