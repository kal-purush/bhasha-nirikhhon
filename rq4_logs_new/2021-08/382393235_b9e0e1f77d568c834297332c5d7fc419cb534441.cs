using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TouchItem : MonoBehaviour
{
    // Start is called before the first frame update
    public GameObject master,Item;
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {

    }
    public void onClickAct() {

        master.GetComponent<MyTimer>().StopTime = Data.StopTime;
        master.GetComponent<MyTimer>().isRemainTimeUsing = false;
        master.GetComponent<MyTimer>().isStopTimeUsing = true;
        Item.SetActive(false);

    }
}