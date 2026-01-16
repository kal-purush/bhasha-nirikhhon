using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MstCharacter : MonoBehaviour {

	[SerializeField]
    private int
        id,
        rarity,
        maxLebel,
        growthType,
        lowerEnergy,
        upperEnergy,
        initialCost;

    [SerializeField]
    private string
        name,
        imageId,
        flavorText;

    public void SetFromCSV(string[] data)
    {
        id          = int.Parse( data[0] );
        name        = data[1];
        imageId     = data[2];
        flavorText  = data[3];
        rarity      = int.Parse( data[4] );
        maxLebel    = int.Parse( data[5] );
        growthType  = int.Parse( data[6] );
        lowerEnergy = int.Parse( data[7] );
        upperEnergy = int.Parse( data[8] );
        initialCost = int.Parse( data[9] );
    }
}