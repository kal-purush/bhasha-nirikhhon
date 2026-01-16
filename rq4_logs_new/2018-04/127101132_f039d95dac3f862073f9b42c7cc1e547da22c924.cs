using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class UnitList : MonoBehaviour {
    public GameObject[] units;

    public enum UnitType
    {
        EArcher,
        ESoldier,
        ETank,
        HArcher,
        HSoldier,
        HTank,
        UArcher,
        USoldier,
        UTank,
    }//i hate remembering array numbers so enums!
}