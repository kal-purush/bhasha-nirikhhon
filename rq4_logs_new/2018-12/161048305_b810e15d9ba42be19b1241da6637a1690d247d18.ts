import Configurable from "./configurable";
import IHtmlAttributes from "./interfaces/IhtmlAttributes";
export declare class Component extends Configurable {
    attributes: IHtmlAttributes;
    parentElement: HTMLElement;
    element: HTMLElement;
    parentSelector: string;
    tag: string;
    parent: Component;
    events: any;
    private _config;
    constructor(config?: any);
    setAttribute(name: string, value: any): void;
    setContent(value: HTMLElement | Component | string | Array<HTMLElement | Component | string>): void;
    addContent(value: HTMLElement | Component | string | Array<HTMLElement | Component | string>): void;
    clearContent(): void;
    show(): void;
    hide(): void;
    protected init(): void;
    protected render(): void;
    protected postRender(): void;
    protected getParentElement(): HTMLElement;
    protected setDefaultProps(): void;
    private innerRender;
}
//# sourceMappingURL=component.d.ts.map