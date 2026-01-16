export interface IAttrs {
}

export class baseTemplate {
    parentElement: JQuery;
    element: JQuery;

    constructor(parentElement: any, template: string) {
        this.parentElement = (typeof(parentElement)==='string') ? $(parentElement) : parentElement;
        this.element = $(template);
    }

    addElement():void {
        this.parentElement.append(this.element);
    }

    addAttr(attrs: IAttrs):void {
            this.element.attr(attrs);
    }

    addStyle(csses: IAttrs):void {
            this.element.css(csses);
    }

    addEvent(nameEvent:string,callBack:Function):void {
        this.element.on(nameEvent, (e) => {
            callBack(e);
        })
    }
}