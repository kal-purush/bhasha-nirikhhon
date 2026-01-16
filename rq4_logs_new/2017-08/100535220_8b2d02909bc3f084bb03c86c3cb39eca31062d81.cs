using System.Collections;
using System.Collections.Generic;
using UnityEngine;

//Base class for an event script.
public class EventScript : MonoBehaviour {

    //The type of event this is. Routine events can be overriden.
    //Time of the event influences when it shows.
    public enum EventType { None, Routine, Special };
    public enum Time { None, Day, Night };

    //Current event type.
    public EventType type = EventType.None;
    public Time time = Time.None;

	//Helper functions for an event

}