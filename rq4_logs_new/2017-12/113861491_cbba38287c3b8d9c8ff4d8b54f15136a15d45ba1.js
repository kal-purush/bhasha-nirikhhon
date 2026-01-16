class EventEmitter2 {
    constructor(){
        this.eventsArray = Object.create(null);
    }

    //methods
    on(eventName, func){
        this.eventsArray[eventName] = func;
    }

    emit(eventName){
        if (eventName in this.eventsArray){
            this.eventsArray[eventName]();
        }
    }

}