using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.IO;
using static Main;

public class Node : MonoBehaviour
{

    public List<Orientations> orientation;
    public String pipeType;
    public int i;
    public int j;

    public Orientations getNewDirection(Orientations originOrientation, bool turnAroundYAxis)
    {
        if (turnAroundYAxis)
        {
            switch (originOrientation)
            {
                case Orientations.North:
                    return Orientations.East;
                case Orientations.East:
                    return Orientations.South;
                case Orientations.South:
                    return Orientations.West;
                case Orientations.West:
                    return Orientations.North;
                default:
                    Debug.Log("Unknown orientation");
                    return Orientations.North;
            }
        }

        //TODO spravit v dalsej verzii
        else
        {
            return Orientations.North;
        }
    }

    public void turnAroundYAxis()
    {

        Debug.Log("old orientation: " + this.orientation[0] + ", " + this.orientation[1]);

        this.orientation = new List<Orientations>() {
                        getNewDirection(this.orientation[0],true),
                        getNewDirection(this.orientation[1],true),
                        };

        Debug.Log("new orientation: " + this.orientation[0] + ", " + this.orientation[1]);

    }

    //toto bude az v dalsej verzii
    public void turnAroundXAxis()
    {

    }

}