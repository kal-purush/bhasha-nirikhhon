using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;

public class PathBuilder : MonoBehaviour
{
    public int InitXWidth = 4;
    public int InitYWidth = 4;
    public NODE_DIRECTION InitDirection = NODE_DIRECTION.UP;
    public Vector2Int Origin;

    public Path _path { get; set; }

	// Use this for initialization
	public void Start ()
    {
        // Build the path out to the Init Widths in the init directions.
        _path = new Path(new Node(Origin.x, Origin.y), InitDirection);

        // Create the initial bounding rect
        Vector2Int offset = new Vector2Int(InitXWidth, InitYWidth);
        Rect bound = new Rect(Origin - offset, offset + offset + Vector2Int.one);

        bool building = true;
        while (building)
        {
            building = false;

            foreach (int i in _path.OuterBranchIDs)
            {
                Branch b = _path.Branches[i];
                if(bound.Contains(b.InNode.VectorLocation))
                {
                    // if the branch in-node is in the bounding rect, set the building flag to true and add another node.
                    // This logic will cause the termination of the outer while loop when all outer branches have in-nodes
                    // outside of the bounding rect.
                    building = true;

                    NODE_DIRECTION dir = b.InNode.OutNode.DirectionOf(b.InNode);
                    b.Add(dir);
                }
            }
        }
	}
	
	// Update is called once per frame
	public void Update () {
		
	}
}