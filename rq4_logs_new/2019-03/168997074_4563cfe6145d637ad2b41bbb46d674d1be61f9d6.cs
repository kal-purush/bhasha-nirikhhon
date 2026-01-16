using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Events;

public class CommandCenter : Destructible
{

    [System.Serializable]
    public class ReportEvent : UnityEvent<Vector3>
    {
    }

    [System.Serializable]
    public class PowerStatusEvent : UnityEvent<Breaker>
    {
    }

    #region Variables

    public ReportEvent reportEvent;
    public PowerStatusEvent powerStatusEvent;


    #endregion

    #region Methods

    private void ReportReaction(Vector3 position)
    {

    }

    private void DetectionReaction(Breaker breaker)
    {

    }

    #endregion

    #region Functions

    void Awake()
    {

        if (reportEvent == null)
            reportEvent = new ReportEvent();

        if (powerStatusEvent == null)
            powerStatusEvent = new PowerStatusEvent();

        reportEvent.AddListener(ReportReaction);
        powerStatusEvent.AddListener(DetectionReaction);

    }

    // Update is called once per frame
    void Update()
    {
        
    }

    #endregion
}