using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlatformGel : Sizeable
{
    public int amount = 25;
    public int capacity = 50;

    public override int getAmount()
    {
        return amount;
    }

    public override bool grow()
    {
        if (amount <= capacity)
        {
            amount++;
            transform.localScale += new Vector3(.1F, .1F, .1F);
            return true;
        }
        return false;
    }

    public override bool shrink()
    {
        if (amount >= 0)
        {
            amount--;
            transform.localScale -= new Vector3(.1F, .1F, .1F);
            return true;
        }
        return false;
    }
}