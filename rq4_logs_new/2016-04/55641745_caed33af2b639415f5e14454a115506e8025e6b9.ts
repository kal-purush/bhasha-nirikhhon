/// <reference path="../typings/browser.d.ts" />
import * as react from "react";
/**
 * Creates and returns an empty React DOM container for a given type of element.
 * Unfortunately necessary because some WinJS components must be a child of the body.
 */
export declare function GetReactDomContainer(selector: string): Element;
/**
 * Renders the given React component into a DOM container based on its name. Useful
 * when the component must be a direct child of the body.
 */
export declare function RenderReactComponent<T>(component: react.ReactElement<T>): react.Component<T, {}> | Element | void;
/**
 * Renders the given ReactWinJS control into a DOM container based on its name. Returns both the
 * React component and WinControl.
 */
export declare function RenderReactWinJSControl<ReactType, WinJSType>(component: JSX.Element): {
    Component: ReactType;
    WinControl: WinJSType;
};