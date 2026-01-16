import { TreeEvents, DataNode, Structure, StructureItem } from './tree';
import { Sortable, OnSortSuccessData } from './base/sortable';
export declare class TreeLine extends Sortable {
    level: number;
    data: DataNode;
    structure: Structure;
    color: string;
    textColor: string;
    offset: number;
    canEdit: boolean;
    count: number;
    text: string;
    events: TreeEvents;
    onSortSuccess(data: OnSortSuccessData): void;
    protected render(): void;
    protected postRender(): void;
    protected createItems(): void;
    protected createItem(value: string, itemCount: number): HTMLElement;
    protected getItemValue(key: string, item: StructureItem): string;
    protected getVisibleItemCount(items: {
        [name: string]: StructureItem;
    }): number;
    protected setDefaultProps(): void;
}
export interface LineData {
    text: string;
    name: string;
    canDrag: boolean;
}
export interface OnLineMoveData extends LineData, OnSortSuccessData {
}
//# sourceMappingURL=treeLine.d.ts.map