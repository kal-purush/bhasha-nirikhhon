using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MJP_TrafficLightOne : MonoBehaviour {

    [SerializeField] float greenTime;
    [SerializeField] float yellowTime;
    [SerializeField] float redTime;

    Renderer greenLight;
    Renderer yellowLight;
    Renderer redLight;

    private float timePassed;

    // Use this for initialization
    void Start () {
        greenLight = GameObject.Find("GreenLight").GetComponent<Renderer>();
        yellowLight = GameObject.Find("YellowLight").GetComponent<Renderer>();
        redLight = GameObject.Find("RedLight").GetComponent<Renderer>();
        ResetTimeSinceLastTransistion();

        greenLight.material.color = Color.grey;
        yellowLight.material.color = Color.grey;
        redLight.material.color = Color.grey;
    }
	
	// Update is called once per frame
	void Update () {
        timePassed += Time.deltaTime;
	}

    public Renderer GetGreenLight()
    {
        return greenLight;
    }

    public Renderer GetYellowLight()
    {
        return yellowLight;
    }

    public Renderer GetRedLight()
    {
        return redLight;
    }

    public float GetTimePassed()
    {
        return timePassed;
    }

    public float GetGreenTime()
    {
        return greenTime;
    }

    public float GetYellowTime()
    {
        return yellowTime;
    }

    public float GetRedTime()
    {
        return redTime;
    }

   public  void ResetTimeSinceLastTransistion()
    {
        timePassed = 0;
    }
}