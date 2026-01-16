import React, { FC } from "react";
import { ICytoscapeContext } from './typescript-types';
interface ICytoscapeComponentProps {
    id?: any;
    className?: any;
    style?: any;
    elements?: any;
    stylesheet?: any;
    layout?: any;
    pan?: any;
    zoom?: any;
    panningEnabled?: any;
    userPanningEnabled?: any;
    minZoom?: any;
    maxZoom?: any;
    zoomingEnabled?: any;
    userZoomingEnabled?: any;
    boxSelectionEnabled?: any;
    autoungrabify?: any;
    autolock?: any;
    autounselectify?: any;
    get?: any;
    toJson?: any;
    diff?: any;
    forEach?: any;
    cy?: any;
    headless?: any;
    styleEnabled?: any;
    hideEdgesOnViewport?: any;
    textureOnViewport?: any;
    motionBlur?: any;
    motionBlurOpacity?: any;
    wheelSensitivity?: any;
    pixelRatio?: any;
    normalizedElements?: any;
}
export declare const CytoscapeComponent: {
    (): JSX.Element;
    displayName: string;
    defaultProps: {
        diff: (a: any, b: any) => boolean;
        get: (obj: any, key: any) => any;
        toJson: (obj: any) => any;
        forEach: (arr: any, iterator: any) => any;
        elements: ({
            data: {
                id: string;
                label: string;
                source?: undefined;
                target?: undefined;
            };
        } | {
            data: {
                id: string;
                source: string;
                target: string;
                label?: undefined;
            };
        })[];
        stylesheet: {
            selector: string;
            style: {
                label: string;
            };
        }[];
        zoom: number;
        pan: {
            x: number;
            y: number;
        };
    };
};
export declare const CytoscapeContext: React.Context<ICytoscapeContext>;
/**
 * The `CytoscapeComponent` is a React component that allows for the declarative creation
 * and modification of a Cytoscape instance, a graph visualisation.
 */
export declare const TestCytoscapeComponent: FC<ICytoscapeComponentProps>;
export declare const normalizedElements: (elements: any) => any;
export {};