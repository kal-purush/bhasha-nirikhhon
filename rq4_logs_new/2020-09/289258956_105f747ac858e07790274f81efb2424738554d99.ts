import { ReactNode, RefObject } from "react";
import { IconDefinition } from "@fortawesome/fontawesome-svg-core";

export type MenuItemId = string | number;
export type MenuItem = {

    id: MenuItemId;
    title: ReactNode;
    /** 
     * Font awesome icon object. For instace: 
     * 
     * ```
     * import { faCoffee } from '@fortawesome/free-solid-svg-icons'
     * ```
     */
    icon: IconDefinition;
    /** Determines where this menu item point to. */
    href?: string;
    /** Determines if the href is pointing a location out if Application's router.  */
    RouteOutOfApp?: boolean;

}

export type SideMenuComponentProps = {
    items?: MenuItem[];
    selectedId?: MenuItemId;
    onItemSelected? : (item: MenuItem)=>void;
}