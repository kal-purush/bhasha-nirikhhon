import { ReactNode, RefObject } from "react";
import { IconDefinition } from "@fortawesome/fontawesome-svg-core";

export type TabItemId = string | number;
export type TabItem = {

    id?: TabItemId;
    title: ReactNode;
    /** 
     * Font awesome icon object. For instace: 
     * 
     * ```
     * import { faCoffee } from '@fortawesome/free-solid-svg-icons'
     * ```
     */
    icon: IconDefinition;
}


export type TabItemComponentProps = TabItem & {
    onClick? : (itemId: TabItemId, ref : RefObject<HTMLDivElement>)=>void;
    selected?: boolean
}

export type TabItemsContainerComponentProps =  {
    selectedId?: TabItemId;
    items?: TabItem[];
    onSelect?:(tabId: TabItem)=>void;
    className?: string;
};