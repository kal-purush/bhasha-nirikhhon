using System.Collections.Generic;
using UnityEngine;

public class Chunk
{
    public Vector3 Location { get; set; }
    public byte[][][] Data { get; set; }
    public List<GameObject> objects { get; set; }

    public Chunk()
    {

    }
}