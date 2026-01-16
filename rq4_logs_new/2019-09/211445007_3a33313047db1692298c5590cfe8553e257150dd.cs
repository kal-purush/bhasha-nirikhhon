using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MergeButton : MonoBehaviour
{
    GameObject[] mergeCircles;

    // Start is called before the first frame update
    void Start()
    {
        mergeCircles = GameObject.FindGameObjectsWithTag("summoncircle");
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    void OnMouseDown()
    {
        List<int> slimeIds = new List<int>();
        foreach (GameObject go in mergeCircles)
        {
            MergeCircle mc = go.GetComponent<MergeCircle>();
            slimeIds.Add(mc.currentSlime.slimeId);
            mc.setCurrentSlime(null);
        }

    }

    void OnMouseUp()
    {
    }
}