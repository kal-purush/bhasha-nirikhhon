import { Component } from "../component";
export default class Select extends Component {
    label: string;
    name: string;
    type: string;
    value: string;
    disabled: boolean;
    onChange: (event: Event, select: Select) => void;
    protected select: HTMLSelectElement;
    setContent(value: HTMLElement | Component | string | HTMLElement[] | Component[] | string[]): void;
    addContent(value: HTMLElement | Component | string | HTMLElement[] | Component[] | string[]): void;
    clearContent(): void;
    render(): void;
    getValue(): string;
    setValue(value: any): void;
    protected setDefaultProps(): void;
}
//# sourceMappingURL=select.d.ts.map