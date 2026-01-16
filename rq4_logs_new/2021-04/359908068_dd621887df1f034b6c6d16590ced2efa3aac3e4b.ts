import 'styled-components';
import { BreakpointsTYPE } from './locales/utils/themeConfig/breakpoints';
interface IPalette {
    light: string;
    main: string;
    dark: string;
}
type greyTYPE = {
    [key: string]: string;
};
declare module 'styled-components' {
    export interface DefaultTheme {
        palette: {
            common: {
                black: string;
                white: string;
                pink: string;
                green: string;
                blue: string;
                purple: string;
                yellow: string;
            };
            primary: IPalette;
            secondary: IPalette;
            bgr: {
                primary: string;
                secondary: string;
                navbar: string;
            };
            text: {
                primary: string;
                secondary: string;
                tertiary: string;
                quaternary: string;
                disabled: string;
            };
            error: {
                main: string;
            };
            warning: {
                main: string;
            };
            info: {
                main: string;
            };
            success: {
                main: string;
            };
            grey: greyTYPE;
        };
        border: {
            color: string;
            radius: string;
        };
        duration: {
            durationFast: string;
            durationNormal: string;
            durationSlow: string;
        };
        typography: {
            h1: {
                fontWeight: number;
                fontSize: string;
                lineHeight: number;
                letterSpacing: number;
            };
            h2: {
                fontWeight: number;
                fontSize: string;
                lineHeight: number;
                letterSpacing: string;
            };
            h3: {
                fontWeight: number;
                fontSize: string;
                lineHeight: number;
                letterSpacing: string;
            };
            body1: {
                fontWeight: number;
                fontSize: string;
                lineHeight: number;
                letterSpacing: string;
            };
            body2: {
                fontWeight: number;
                fontSize: string;
                lineHeight: number;
                letterSpacing: string;
            };
            button: {
                fontWeight: number;
                fontSize: string;
                lineHeight: number;
                letterSpacing: string;
            };
        };
        space: string[];
        transitions: {
            easing: {
                easeInOut: string;
                easeOut: string;
                easeIn: string;
                sharp: string;
            };
        };
        breakpoints: BreakpointsTYPE;
    }
}