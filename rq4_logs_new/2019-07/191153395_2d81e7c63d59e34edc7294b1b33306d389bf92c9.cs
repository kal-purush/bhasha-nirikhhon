using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MapGeneratorHole : MonoBehaviour {

    public GameObject CurrentSegment;
    public GameObject[] RoadSegments;
    public bool createdNext;

    // Use this for initialization
    void Start () {
        createdNext = false;
    }

    int countDown = 150 * 10;
	// Update is called once per frame
	void FixedUpdate () {
        if ( countDown-- <= 0)
        {
            Destroy(gameObject);
        }
	}

    private void OnCollisionStay(Collision other)
    {
       
            //Debug.Log("Spawn Map");
            if (other.gameObject.tag.Equals("Player") && !createdNext)
            {
                SpawnMap();
            }

    }

    // Spawn new map
    public void SpawnMap()
    {

        int randomed = Random.Range(0, RoadSegments.Length);

        var lastTileBounds = GetComponent<MeshFilter>().mesh.bounds;

        Instantiate(RoadSegments[randomed],
            new Vector3(transform.position.x - lastTileBounds.size.x * transform.localScale.x,
            transform.position.y,
            transform.position.z),
            transform.rotation
            );

        createdNext = true;

    }

}