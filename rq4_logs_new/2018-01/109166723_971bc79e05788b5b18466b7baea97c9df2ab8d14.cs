using System.Collections;
using System.Collections.Generic;
using UnityEngine;


// REWORK NEEDED
public class ClothBehavior : MonoBehaviour
{

    public Blane.CreateCloth _cloth = new Blane.CreateCloth();

    public Vector3 particlePosition;

    public GameObject particleObject;
    public GameObject anchorObject;

    public List<GameObject> particleObjectList = new List<GameObject>();
    public List<GameObject> anchorObjectList = new List<GameObject>();

    // Sets UP Particle Objects to be the Particle Position
    void Start()
    {
        _cloth.Initialize();
        _cloth.SetParticlePosition();

        //  ***
        //  Loop sets up particle objects to equal the position of the given particle
        for (int m = 0; m < 16; m++)
        {
            particleObject = GameObject.CreatePrimitive(PrimitiveType.Sphere);
            particleObjectList.Add(particleObject);
            particleObjectList[m].transform.position = _cloth.particleList[m].position;
            //Debug.Log("2nd Particle Position Check: " + particleObjectList[m].transform.position.ToString());

            if (m >= 1)
            {
                anchorObject = particleObjectList[m - 1];
                anchorObjectList.Add(anchorObject);
                anchorObjectList[m - 1].transform.position = _cloth.anchorList[m - 1].position;
            }
        }
    }


    // update particle position
    void Update()
    {
        for (int u = 0; u < 16; u++)
        {
            _cloth.particleList[u].Update(Time.deltaTime);
        }
    }
}