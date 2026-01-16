using UnityEngine;
using System.Collections;

public class mapchip : MonoBehaviour
{
    public float chipsize = 0.16f;
    public GameObject[] floor1;

    private int map = 0;

    private float x = 0;
    private float y = 0;

    public int[][] map_array = new int[][] {
        new int[] { 1, 1, 1, 1, 1 },
        new int[] { 1, 1, 1, 1, 1 },
        new int[] { 1, 1, 1, 1, 1 },
        new int[] { 1, 1, 1, 1, 1 },
        new int[] { 1, 1, 1, 1, 1 }
    };

    void Awake()
    {

    }

    // Use this for initialization
    void Start()
    {
        //for (int i = 0; i < 10; i++)
        //{
        //    Vector3 v = new Vector3(0.1f * i, 0, 0);
        //    Quaternion q = new Quaternion();
        //    Instantiate(sp, v, q);
        //}
        foreach (int[] array in map_array)
        {
            foreach (int s in array)
            {
                Instantiate(floor1[s], new Vector3(x, y, 0), Quaternion.identity);
                x += 1;
            }
            x = 0;
            y += 1;
        }

    }
    // Update is called once per frame
    void Update()
    {

    }
}