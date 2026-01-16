using UnityEngine;
using System.Collections;
using UnityEngine.UI;
using UnityEngine.Events;
using UnityEngine.EventSystems;

public class EventSystemManager : MonoBehaviour 
{

    void Start()
    {
        //SetEventTriggerState(this.gameObject.GetComponent<EventTrigger>(), EventTriggerType.PointerUp, "MethodnameToSetState", UnityEventCallState.Off);
    }


    public static void SetEventTriggerState(EventTrigger ET, EventTriggerType ETType, string MethodName, UnityEventCallState NewState)
    {
        for (int i=0; i<ET.triggers.Count; i++) 
        {
            EventTrigger.Entry Trigger=ET.triggers[i];
            Debug.Log("    Triggers[i] "+Trigger);
            Debug.Log("    eventID "+Trigger.eventID);
            EventTrigger.TriggerEvent CB=Trigger.callback;
            Debug.Log("    callback "+CB);
            for (int j=0; j<CB.GetPersistentEventCount(); j++) 
            {
                Debug.Log("        "+CB.GetPersistentMethodName(j)+" "+CB.GetPersistentTarget(j)+" "+Trigger.eventID);
                if (CB.GetPersistentMethodName(j)==MethodName && Trigger.eventID==ETType)
                {
                    Debug.Log("Set State to "+NewState);
                    CB.SetPersistentListenerState(j, NewState);
                }
            }
        }
    }
}