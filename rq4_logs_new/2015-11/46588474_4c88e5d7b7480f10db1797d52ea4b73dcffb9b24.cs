using UnityEngine;
using System.Collections;


public class NightManager : MonoBehaviour
{

    public static NightManager instance;

    public static NightManager Instance
    {
        get { return instance; }
    }

    // Use this for initialization
    void Start()
    {
        if (instance == null)
            instance = this;
    }

    // Update is called once per frame
    void Update()
    {

    }

    public void End(GameObject coll)
    {
        Memory.Instance.SetStatistics(coll.GetComponent<State>().GetMoods());
        Destroy(coll);
        Application.LoadLevel(0);
    }

}