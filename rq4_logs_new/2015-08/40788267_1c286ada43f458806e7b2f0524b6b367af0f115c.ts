///<reference path="../Utils/Collections/List.ts"/>
///<reference path="../core/GameEvents.ts"/>

class ActionHelper {
	
	static getDispatchActions(event: core.IEvent, eventMgr: core.EventManager): Array<Action> {
		
        var result: Array<Action> = [];
        var handlerObj : core.IHandlerObj;
    	for (var handlerIdx in eventMgr.handlers._list) {
            handlerObj = eventMgr.handlers._list[handlerIdx];
    		if (handlerObj.eventType === event.eventType) {
                result.push(new DispatchEventAction(event, handlerObj));
    		}
    	}
        return result;
	}
	
}