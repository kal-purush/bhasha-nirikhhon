
/// <reference path='../../typings/react/react.d.ts' />

declare interface JsonScriptAttributes {
    id: string;
    data: string;
}

declare interface DocumentProps {
    confs?: string[];
    title?: string;
    styles?: string[];
    jsonScriptData?: JsonScriptAttributes[];
    layout?: any;
}

declare abstract class ComposerComponent<P, S> extends React.Component<P, S> {
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

declare class ComposerDocument<Props extends DocumentProps, States> extends ComposerComponent<Props, States> {}
declare class ComposerLayout<Props extends DocumentProps, States> extends ComposerComponent<Props, States> {}
declare class ComposerContent<Props extends DocumentProps, States> extends ComposerComponent<Props, States> {
    static fetch(): Promise<any>;
}

declare interface ComposerComponents {
    [name: string]: typeof ComposerContent;
}

declare interface Contents {
    [index: string]: JSX.Element;
}

declare interface PageComponents {
    Document: ComposerComponents;
    Layout: ComposerComponents;
    Content: ComposerComponents;
}