import { CKHtmlModel } from './dom/CKHtmlModel';
import { DOMManipulator } from './dom/CKDOMManipulator';

export class CKComponent {
    private htmlModel: CKHtmlModel;
    private static lastId: number = 0;
    private id: number;

    constructor(protected htmlRef: string, private anchor: string) {
        this.id = CKComponent.lastId++;
    }

    public createView() : Promise<any> {
        let instance: CKComponent = this;
        return new Promise<any>((resolve, reject) => {
            let attr: string = "ckcomponent" + instance.id;
            try {
            jQuery.get(instance.htmlRef, function (html) {
                instance.htmlModel = new CKHtmlModel(instance.anchor, attr, html, instance);
                resolve();
            });
            } catch(error) {
                reject(error);
            }
        });
    }

    protected applyScopeChange() {
        this.htmlModel.applyScopeChange(this);
    }
}