using UnityEngine;
using System;
using System.Collections;
using System.Collections.Generic;

public class Nekromanta : MonoBehaviour {

    public List<GameObject> trasa;
    private int cel = 1;
    private bool rusza_sie = false;
    public float szybkosc = 3.0f;

    // Use this for initialization
    void Start () {
        rusza_sie = true;
        cel = 1;
        this.gameObject.transform.position = trasa[0].transform.position;
	}

    // Update is called once per frame
    void Update() {
        if (rusza_sie)
        {
            //Debug.Log(cel);
            if(Vector3.Distance(this.gameObject.transform.position, trasa[cel].transform.position) > 0.1)
            {
                this.transform.position = Vector3.MoveTowards(this.transform.position, trasa[cel].transform.position, szybkosc * Time.deltaTime);
            }
            else
            {
                StartCoroutine(czekaj(int.Parse(trasa[cel].name.Substring(trasa[cel].name.Length - 1))));
            }
        }
    }



    public IEnumerator czekaj(int czas)
    {
        rusza_sie = false;
        cel++;
        if (cel >= trasa.Count)
        {
            cel = 0;
            //Debug.Log("!" + cel);
        }
        yield return new WaitForSeconds(czas);
        rusza_sie = true;
    }
}