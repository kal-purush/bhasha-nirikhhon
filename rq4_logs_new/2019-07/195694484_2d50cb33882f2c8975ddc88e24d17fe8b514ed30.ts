import { RadSideDrawer } from "nativescript-ui-sidedrawer";
import { EventData } from "tns-core-modules/data/observable";
import { View } from "tns-core-modules/ui/core/view";

let drawerComponent: RadSideDrawer;

export function onLoaded(args: EventData): void {
    drawerComponent = <RadSideDrawer>args.object;
}

export function onDrawerOpening() {
    if (drawerComponent) {
        const stack: View = drawerComponent.getViewById("stack");
        if (stack) {
            const bar1: View = stack.getViewById("bar1");
            const bar2: View = stack.getViewById("bar2");
            const bar3: View = stack.getViewById("bar3");

            if (bar1 && bar2 && bar3) {
                bar1.className = "bar bar1 menu-bar-on";
                bar2.className = "bar bar2 menu-bar-on";
                bar3.className = "bar bar3 menu-bar-on";
            }
        }
    }
}

export function onDrawerClosing() {
    if (drawerComponent) {
        const stack: View = drawerComponent.getViewById("stack");
        if (stack) {
            const bar1: View = stack.getViewById("bar1");
            const bar2: View = stack.getViewById("bar2");
            const bar3: View = stack.getViewById("bar3");

            if (bar1 && bar2 && bar3) {
                bar1.className = "bar bar1 menu-bar-off";
                bar2.className = "bar bar2 menu-bar-off";
                bar3.className = "bar bar3 menu-bar-off";
            }
        }
    }
}