using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TitleMenuNowLoading : WordManager
{
    [SerializeField] string textTitle = "Now Loading";

    // Start is called before the first frame update
    void Start()
    {
        StartUp();
        InstantSplitTest(textTitle);
        SetTextObjCenter();
        OnTextWavePlaySetUp(5f);
        LastStartUp();

        transform.localRotation =
            Quaternion.AngleAxis(-90, Vector3.up) * transform.localRotation;
    }

    // Update is called once per frame
    void Update()
    {
        updata();
    }
}