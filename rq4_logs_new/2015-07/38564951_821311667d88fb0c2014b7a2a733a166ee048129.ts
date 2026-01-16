
/// <reference path='../../typings/es6-promise/es6-promise.d.ts'/>

import { Component } from 'react';

export interface JsonScriptAttributes {
    id: string;
    data: string;
}

export interface DocumentProps {
    confs?: string[];
    title?: string;
    styles?: string[];
    jsonScriptData?: JsonScriptAttributes[];
    layout?: any;
}

export abstract class ComposerComponent<P, S> extends Component<P, S> {
    /**
     * This static property is a native readonly JS property and it is automatically set to the
     * constructor's name.
     */
    public static name: string;

    /**
     * Some decorators wraps a class with their own class and thus alters the name of a
     * constructor. Please set this property to supercede such changes.
     */
    public static className: string;
}

export class ComposerDocument<Props extends DocumentProps, States> extends ComposerComponent<Props, States> {}

export class ComposerLayout<Props, States> extends ComposerComponent<Props, States> {}

export class ComposerContent<Props, States> extends ComposerComponent<Props, States> {
    static fetch(): Promise<any> {
        return new Promise((resolve, reject) => {
           resolve();
        });
    }
}

export interface Info {
    importPath: string;
    component: typeof Component;
}

export interface DocumentInfo extends Info {
    component: typeof ComposerDocument;
}

export interface LayoutInfo extends Info {
    component: typeof ComposerLayout;
}

export interface ContentInfo extends Info {
    component: typeof ComposerContent;
}

export interface StoredContentInfos {
    [index: string]: ContentInfo;
}

export interface ProvidedContentInfos {
    [index: string]: ContentInfo | typeof ComposerContent;
}