/// <reference path="../react/index.d.ts" />

declare var Radium: _Radium.RadiumStatic;

declare namespace _Radium {
    import React = __React;

    interface CssClass {
        [name: string]: React.CSSProperties;
    }

    interface StyleProps {
        rules?: CssClass;
        scopeSelector?: string;
    }

    interface StyleRootProps extends React.HTMLAttributes {
        style?: React.CSSProperties,
    }

    interface StyleClass extends React.ComponentClass<StyleProps> {}
    interface StyleRootClass extends React.ComponentClass<StyleRootProps> {}

    interface PluginConfig {
        addCSS: (css: string) => {remove: () => void};
        appendImportantToEachValue: (style: Object) => Object;
        componentName: string;
        config: Config;
        cssRuleSetToString: (
          selector: string,
          rules: Object,
          userAgent?: string
        ) => string;
        getComponentField: (key: string) => any;
        getGlobalState: (key: string) => any;
        getState: (stateKey: string, elementKey?: string) => any;
        hash: (data: string) => string;
        isNestedStyle: (value: any) => boolean;
        mergeStyles: (styles: Array<Object>) => Object;
        props: Object;
        setState: (stateKey: string, value: any, elementKey?: string) => void;
        style: Object;
        ExecutionEnvironment: {
            canUseEventListeners: boolean,
            canUseDOM: boolean,
        };
    }

    interface PluginResult {
        componentFields?: Object;
        globalState?: Object;
        props?: Object;
        style?: Object;
    }

    interface Plugin {
        (config: PluginConfig): PluginResult
    }

    interface Config {
        matchMedia: Function;
        plugin: Array<Plugin>;
        userAgent: string;
    }

    export class ElementClass extends React.Component<any, any> { }

    interface RadiumStatic {
        <T>(comp: T): T;
        (config: Config): <T>(comp: T) => T;

        getState: (state: Object,
                   elementKey: string,
                   value: ':active' | ':focus' | ':hover') => boolean;

        keyframes: (keyframes: { [key: string]: StyleProps },
                    name?: string) => any;

        StyleRoot: StyleRootClass;
        Style: StyleClass;
    }
}

declare module 'radium' {
    export = Radium;
}