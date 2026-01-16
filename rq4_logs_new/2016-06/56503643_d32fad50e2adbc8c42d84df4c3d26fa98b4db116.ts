/// <reference path="../core/dom/domobject.ts" />

/**
 * Button
 */
class Button extends DOMObject {
        
    constructor(x:number, y:number, HTMLtagName: string) {
       super(x,y, HTMLtagName);
       this.htmlElement.addEventListener("click", (e) => this.handleClick(e));
    }

    protected handleClick(event: MouseEvent) : void {
        
    }
    
}