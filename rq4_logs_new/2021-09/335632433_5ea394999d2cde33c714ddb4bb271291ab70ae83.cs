using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class RunSound : MonoBehaviour
{
    public GameObject R;
    private CriAtomSource RAshioto;
    public GameObject L;
    private CriAtomSource LAshioto;
    // Start is called before the first frame update
    void Start()
    {
        RAshioto = R.GetComponent<CriAtomSource>();
        LAshioto = L.GetComponent<CriAtomSource>();
    }

    // Update is called once per frame
    void Update()
    {

    }

    void RRunFlag()
    {
        RAshioto.Play();
    }
    void LRunFlag()
    {
        LAshioto.Play();
    }
}