using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Item_Garment_Tunic : Item_Garment {

	// Use this for initialization
	void Start () {
        bodyPartCoverage.Add(RPGStatType.Chest);
        bodyPartCoverage.Add(RPGStatType.Stomach);
        bodyPartCoverage.Add(RPGStatType.Pelvis);
        bodyPartCoverage.Add(RPGStatType.UpperBack);
        bodyPartCoverage.Add(RPGStatType.LowerBack);
        bodyPartCoverage.Add(RPGStatType.LeftShoulder);
        bodyPartCoverage.Add(RPGStatType.RightShoulder);
        bodyPartCoverage.Add(RPGStatType.LeftUpperArm);
        bodyPartCoverage.Add(RPGStatType.RightUpperArm);
        bodyPartCoverage.Add(RPGStatType.LeftForearm);
        bodyPartCoverage.Add(RPGStatType.RightForearm);
    }

    // Update is called once per frame
    void Update () {
		
	}
}