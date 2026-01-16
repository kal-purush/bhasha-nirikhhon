
using Unity.VisualScripting;
using UnityEngine;

public class OldPositions

{
    public Vector3 Position;

    public int tick;

    public OldPositions(Vector3 positions, int tick)
    {
        Position = positions;
        this.tick = tick;
    }
}