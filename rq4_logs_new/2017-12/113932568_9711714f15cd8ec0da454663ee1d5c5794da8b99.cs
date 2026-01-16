using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Place {
    private static Place instance = null;

    public int placeNum;

    private Place()
    {
        placeNum = 1;
    }

    public static Place getInstance()
    {
        if (instance == null)
        {
            instance = new Place();
        }
        return instance;
    }

    public int getplaceNum() // get place num
    {
        return placeNum;
    }

    public void setplaceNum(int num) // set place num
    {
        placeNum = num;
    }

}