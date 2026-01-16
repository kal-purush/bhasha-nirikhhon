using System.Collections;
using System.Collections.Generic;
using TreeEditor;
using UnityEngine;

public class WindManager : MonoBehaviour
{
    private Perlin PerlinX = new Perlin();
    private Perlin PerlinY = new Perlin();
    private Perlin PerlinZ = new Perlin();

    private Perlin Perlin = new Perlin();
    [SerializeField] private List<Vector3> IslandPositions; // For creating drag towards locations.

    [SerializeField] private Vector3 LevelSize = new Vector3(1024f, 256f, 1024f);
    [SerializeField] private float LevelScale = 1f;


    void Start()
    {
        PerlinX.SetSeed(0);
        PerlinY.SetSeed(1);
        PerlinZ.SetSeed(2);
    }

    //private float GetNoise(Vector3 pos)
    //{
    //    return Perlin.Noise(pos.x / (LevelSize.x * LevelScale),
    //        pos.y / (LevelSize.y * LevelScale),
    //        pos.z / (LevelSize.z * LevelScale)); // Apply a scale to make the perlin noise a bit more spread out.
    //}

    private Vector3 GetWindAtPoint(Vector3 point, Vector3 size)
    {

        float noisePerlinX = PerlinX.Noise(point.x / size.x, point.y / size.y, point.z / size.z);
        float noisePerlinY = PerlinY.Noise(point.x / size.x, point.y / size.y, point.z / size.z);
        float noisePerlinZ = PerlinZ.Noise(point.x / size.x, point.y / size.y, point.z / size.z);

        return new Vector3(noisePerlinX, noisePerlinY, noisePerlinZ);
    }

    private void OnDrawGizmos()
    {
        for (int x = 0; x < 32; x+=2)
        {
            for (int y = 0; y < 16; y+=2)
            {
                for (int z = 0; z < 32; z+=2)
                {
                    var posVector = new Vector3(x,y,z);
                    var windVector = GetWindAtPoint(posVector, new Vector3(32, 16, 32));
                    //Gizmos.color = new Color(windVector.magnitude, 0f, 0f);
                    Gizmos.DrawLine(posVector, windVector + posVector);
                    Gizmos.DrawSphere(posVector, 0.05f);
                }
            }
        }
    }
}