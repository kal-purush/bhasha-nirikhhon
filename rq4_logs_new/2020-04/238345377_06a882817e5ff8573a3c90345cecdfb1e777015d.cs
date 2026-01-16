using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PathNode : MonoBehaviour
{
    // Create static adj matrix 

    static PathNode[,] mstrGrid;

    private List<PathNode> neighbors;
    private int vIdx; // Index of vertex column of which this node represents

    // Start is called before the first frame update. new path nodes should specify vertex count
    void Start(int vertsLen, int currentVertexNum)
    {
        vIdx = currentVertexNum;
        mstrGrid = new PathNode[vertsLen, vertsLen];
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    bool isPlayerLocation()
    {
        // True if the player is colliding with this node.
    }
}