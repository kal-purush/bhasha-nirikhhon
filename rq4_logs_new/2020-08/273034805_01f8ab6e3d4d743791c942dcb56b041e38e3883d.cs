using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class MapUi : MonoBehaviour
{
    public Transform roomImageParent;
    public Color visitedColor, notvisitedColor;
    public Map map;
    public GameObject mapobj;
    
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        if (map == null) { map = GameObject.Find("map").GetComponent<Map>(); }
        if (map.GetComponent<Map>().mapui == null) { map.GetComponent<Map>().mapui  = GetComponent<MapUi>(); }
    }

    public void SetRoomImage(int whichroom,Room room) 
    {
        if (roomImageParent.childCount > whichroom)
        {
            roomImageParent.GetChild(whichroom).GetComponent<RawImage>().color = room.GetVisited() ? visitedColor : notvisitedColor;
            roomImageParent.GetChild(whichroom).GetChild(0).gameObject.active = room.GetCollectiblesCount() > 0;
        }
    }


}