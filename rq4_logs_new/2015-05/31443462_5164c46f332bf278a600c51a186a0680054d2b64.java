package org.jivesoftware.openfire.interceptor;

import java.util.HashMap;
import java.util.Map;

public enum EEventType {
	All("All"),
	Incoming("Incoming"),
	Outgoing("Outgoing"),
	Processed("Processed"),
	Unprocessed("Unprocessed");
	
	private static final Map<String, EEventType> eventMap = new HashMap<String, EEventType>();
	private String eventName;

    static {
        for (EEventType event : EEventType.values())
            eventMap.put(event.getEventName(), event);
    }
	
    private EEventType(String name) {
    	this.eventName = name;
    }
	
    public String getEventName() {
    	return this.eventName;
    }
	public static EEventType fromString(String name) {
		return eventMap.get(name);
	}
}