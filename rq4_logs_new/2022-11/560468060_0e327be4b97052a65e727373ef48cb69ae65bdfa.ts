import "styled-components";

declare module "styled-components" {
    export interface DefaultTheme {
        mode: string;
        body: string;
        headerBtn: string;
        titleBtn: string;
        card_radius: string;
        text: string;
        text_hover: string;
        border: string;
        disable: string;
        item: string;
        success: string;
    }
}