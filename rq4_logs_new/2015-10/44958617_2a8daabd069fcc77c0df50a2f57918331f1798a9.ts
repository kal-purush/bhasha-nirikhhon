namespace YJMCNT {
    /**
     * CounterView
     */
    class CounterView extends Core.View {
        counter:Counter;
        
        constructor() {
            super();
            this.counter = new Counter();
            this.counter.addObserver(this);
        }
        render() {
            var counter = document.createElement("div");
            var countView = document.createElement("span");
            countView.classList.add("count");
            countView.innerText = "count: "+this.counter.show().toString();
            
            counter.appendChild(countView);
            return counter;
        }
        update() {
            this.notifyObservers();
        }
    }
}