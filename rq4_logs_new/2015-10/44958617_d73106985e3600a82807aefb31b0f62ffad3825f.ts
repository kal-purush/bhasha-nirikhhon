namespace YJMCNT.Core {
    /**
     * Model
     */
    export class Model implements Core.Subject{
        observerList:Core.Observer[]; 

        constructor() {
            this.observerList = [];
        }
        
        addObserver(observer:Observer):void {
            this.observerList.push(observer);
        }
        notifyObservers():void {
            for (var index = 0; index < this.observerList.length; index++) {
                var observer = this.observerList[index];
                observer.update();
            }
        }
    }
}