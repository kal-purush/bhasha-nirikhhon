using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Loom2Completion : MonoBehaviour
{

    public bool onRoof;
    public GameObject[] particleObjects;
    public float riseTime;


    public ParticleBells bells;

    private Vector3 endPos;

    private bool risen;
    private bool faded;
    
    void Update()
    {
        if (onRoof)
        {
            onRoof = false;
            bells.end = true;
            StartCoroutine(riseUP());
            Debug.Log("rising up");
        }
        
        
        if (risen)
        {
            risen = false;
            StartCoroutine(fadeAway());
        }

        if (faded)
        {
            faded = false;
            //GetComponent<Rigidbody>().useGravity = true;
        }
    }

    IEnumerator riseUP()
    {
        transform.parent = null;
        Vector3 startPosition = particleObjects[0].transform.position;
        Vector3 fireballStart = transform.position;
        endPos = new Vector3(startPosition.x, startPosition.y + 12f, startPosition.z);
        
        float t = 0;
        while (t < 1)
        {
            t += Time.deltaTime / riseTime;
            for (int i = 0; i < particleObjects.Length; i++)
            {
                particleObjects[i].transform.position = Vector3.Lerp(startPosition, endPos, t);
                Debug.Log("rising further");
            }
            transform.position = Vector3.Lerp(fireballStart, endPos, t);
            yield return null;
        }

        risen = true;
    }

    IEnumerator fadeAway()
    {
        Vector3 startScale = particleObjects[0].transform.localScale;

        float t = 0;
        while (t < 1)
        {
            t += Time.deltaTime / riseTime;
            for (int i = 0; i < particleObjects.Length; i++)
            {
                particleObjects[i].transform.localScale = Vector3.Lerp(startScale, Vector3.zero, t);
            }
            yield return null;
        }
        
        for (int i = 0; i < particleObjects.Length; i++)
        {
            Destroy(particleObjects[i]);
        }

        transform.localScale = new Vector3(3.5f, 3.5f, 3.5f);

        faded = true;
    }
    
}