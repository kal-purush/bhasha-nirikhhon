using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlantController : MonoBehaviour
{
    public Material dirtMat;
    public float settleSpeed;

    private bool inRange;
    private bool settled;

    private float startDeformScale;
    private float startDirtScale;
    private float startScrollSpeed1;
    private float startScrollSpeed2;

    void Start()
    {
        startDeformScale = dirtMat.GetFloat("_DeformScale");
        startDirtScale = dirtMat.GetFloat("_DirtScale");
        startScrollSpeed1 = dirtMat.GetFloat("_ScrollSpeed1");
        startScrollSpeed2 = dirtMat.GetFloat("_ScrollSpeed2");
    }
    

    // Update is called once per frame
    void Update()
    {
        if (!settled && inRange && Input.GetKeyDown(KeyCode.E))
        {
            settled = true;
            StartCoroutine(SettleDirt());
        }
    }

    void OnTriggerEnter(Collider coll)
    {
        if (coll.CompareTag("Player"))
        {
            inRange = true;
        }
    }

    void OnTriggerExit(Collider coll)
    {
        if (coll.CompareTag("Player"))
        {
            inRange = false;
        }
    }

    IEnumerator SettleDirt()
    {
        float t = 0;
        while (t < 1f)
        {
            t += Time.deltaTime * settleSpeed;
            
            dirtMat.SetFloat("_DeformScale", Mathf.Lerp(startDeformScale, 0.001f, t));
            dirtMat.SetFloat("_DirtScale", Mathf.Lerp(startDirtScale, 1f, t));
            

            yield return null;
        }
        
        dirtMat.SetFloat("_ScrollSpeed1", 0);
        dirtMat.SetFloat("_ScrollSpeed2", 0);
    }
}
